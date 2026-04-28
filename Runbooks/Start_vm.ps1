param(
    [Parameter(Mandatory=$false)]
    [String] $WebhookData
)

try {
    # Enable logging
    $VerbosePreference = "Continue"
    $ErrorActionPreference = "Stop"

    Write-Output "Starting VM start operation..."

    # Connect using managed identity
    $null = Connect-AzAccount -Identity
    Write-Output "Successfully connected with managed identity"

    # Get the VM
    $vm = Get-AzVM -ResourceGroupName "cdiouzima" -Name "uzima-copied" -Status -ErrorAction Stop
    Write-Output "Current VM status: $($vm.Statuses[1].Code)"

    # Check if VM is already running
    if ($vm.Statuses[1].Code -eq "PowerState/running") {
        Write-Output "VM is already running"
    } else {
        # Start the VM
        Write-Output "Starting VM..."
        Start-AzVM -ResourceGroupName "cdiouzima" -Name "uzima-copied" -ErrorAction Stop
        Write-Output "VM start command sent successfully"
    }
}
catch {
    Write-Error "Error in runbook: $_"
    Write-Error "Error details: $($_.Exception.Message)"
    Write-Error "Error at line: $($_.InvocationInfo.ScriptLineNumber)"
    Write-Error "Line: $($_.InvocationInfo.Line.Trim())"
    throw $_
}
finally {
    Write-Output "Runbook execution completed"
}