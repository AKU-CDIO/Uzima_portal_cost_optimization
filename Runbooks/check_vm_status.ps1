param(
    [Parameter(Mandatory=$false)]
    [string]$WebhookData
)

# Enable verbose logging
$VerbosePreference = "Continue"
$ErrorActionPreference = "Stop"

# Default values
$SubscriptionId = "a5d4ffbe-d287-4dd1-86c9-f1214fe751d6"
$ResourceGroupName = "CDIOUZIMA"
$VMName = "UZIMA"
$JobId = $null
$CallbackUrl = $null

# Parse webhook data if provided
if (-not [string]::IsNullOrEmpty($WebhookData)) {
    $webhookBody = $WebhookData | ConvertFrom-Json -ErrorAction SilentlyContinue
    if ($webhookBody) {
        # Check if this is a job status check
        if ($webhookBody.JobId) {
            $JobId = $webhookBody.JobId
            Write-Output "Checking status of job: $JobId"
        }
        # Parse other parameters if provided
        if ($webhookBody.SubscriptionId) { $SubscriptionId = $webhookBody.SubscriptionId }
        if ($webhookBody.ResourceGroupName) { $ResourceGroupName = $webhookBody.ResourceGroupName }
        if ($webhookBody.VMName) { $VMName = $webhookBody.VMName }
        if ($webhookBody.CallbackUrl) { $CallbackUrl = $webhookBody.CallbackUrl }
    }
}

# Function to send callback
function Send-Callback {
    param (
        [string]$url,
        [hashtable]$data
    )
    try {
        $body = $data | ConvertTo-Json -Depth 5
        Write-Output "Sending callback to $url"
        Write-Output "Callback data: $body"
        
        $params = @{
            Uri = $url
            Method = 'POST'
            Body = $body
            ContentType = 'application/json'
            UseBasicParsing = $true
        }
        
        # Add retry logic
        $maxRetries = 3
        $retryCount = 0
        $success = $false
        
        do {
            try {
                $response = Invoke-RestMethod @params
                $success = $true
                Write-Output "Callback successful"
                return $response
            }
            catch {
                $retryCount++
                if ($retryCount -ge $maxRetries) {
                    Write-Error "Failed to send callback after $maxRetries attempts: $_"
                    throw
                }
                Write-Warning "Callback attempt $retryCount failed. Retrying in 2 seconds..."
                Start-Sleep -Seconds 2
            }
        } while (-not $success -and $retryCount -lt $maxRetries)
    }
    catch {
        Write-Error "Error in Send-Callback: $_"
        throw
    }
}

try {
    # Connect to Azure using managed identity
    $null = Connect-AzAccount -Identity
    Write-Output "Successfully connected with managed identity"

    # Set subscription context
    $context = Set-AzContext -SubscriptionId $SubscriptionId
    Write-Output "Using subscription: $($context.Subscription.Name)"

    # If this is a job status check, return the status
    if ($JobId) {
        Write-Output "Retrieving job status for: $JobId"
        $job = Get-AzAutomationJob -Id $JobId -ResourceGroupName $ResourceGroupName -AutomationAccountName "YourAutomationAccount" -ErrorAction Stop
        
        # If job is completed, get the output
        if ($job.Status -eq "Completed") {
            $jobOutput = Get-AzAutomationJobOutput -Id $job.JobId -ResourceGroupName $ResourceGroupName -AutomationAccountName "YourAutomationAccount" -Stream "Output" | Get-AzAutomationJobOutputRecord
            $outputObject = @{
                Status = "Completed"
                Output = $jobOutput.Summary
                Timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
            }
            
            # If there's a callback URL, send the result there
            if ($CallbackUrl) {
                Send-Callback -url $CallbackUrl -data $outputObject
            }
            
            return $outputObject | ConvertTo-Json
        } else {
            # Job is still running
            $status = @{
                Status = "Running"
                JobId = $job.JobId
                StatusMessage = $job.Status
                Timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
            }
            
            # If there's a callback URL, send the status update
            if ($CallbackUrl) {
                Send-Callback -url $CallbackUrl -data $status
            }
            
            return $status | ConvertTo-Json
        }
    }

    # If no job ID, start a new status check
    Write-Output "Starting VM status check for $VMName..."
    $vm = Get-AzVM -ResourceGroupName $ResourceGroupName -Name $VMName -Status -ErrorAction Stop
    $powerStatus = ($vm.Statuses | Where-Object { $_.Code -like "PowerState*" }).DisplayStatus
    
    $status = @{
        VMName = $VMName
        PowerState = $powerStatus
        LastUpdated = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
        Status = "Completed"
    }
    
    # If there's a callback URL, send the result there
    if ($CallbackUrl) {
        Send-Callback -url $CallbackUrl -data $status
    }
    
    # Return the status
    return $status | ConvertTo-Json -Depth 10
}
catch {
    $errorMessage = $_.Exception.Message
    $errorDetails = @{
        Status = "Failed"
        Error = $errorMessage
        Timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    }
    
    Write-Error "Error in runbook: $errorMessage"
    
    # If there's a callback URL, send the error there
    if ($CallbackUrl) {
        try {
            Send-Callback -url $CallbackUrl -data $errorDetails
        }
        catch {
            Write-Error "Failed to send error callback: $_"
        }
    }
    
    # Return error details
    return $errorDetails | ConvertTo-Json
}