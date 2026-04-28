param(
    [Parameter(Mandatory=$false)]
    [string]$SubscriptionId = "a5d4ffbe-d287-4dd1-86c9-f1214fe751d6",
    
    [Parameter(Mandatory=$false)]
    [string]$ResourceGroupName = "CDIOUZIMA",
    
    [Parameter(Mandatory=$false)]
    [string]$VMName = "uzima-copied",
    
    [Parameter(Mandatory=$false)]
    [int]$Port = 3389,
    
    [Parameter(Mandatory=$false)]
    [int]$DurationHours = 3
)

try {
    # 1. Connect to Azure
    Write-Host "[LOG] Step 1: Connecting to Azure with managed identity..." -ForegroundColor Gray
    $null = Connect-AzAccount -Identity -ErrorAction Stop
    
    # 2. Set subscription context
    Write-Host "[LOG] Step 2: Setting subscription context to $SubscriptionId..." -ForegroundColor Gray
    $context = Set-AzContext -SubscriptionId $SubscriptionId -ErrorAction Stop
    Write-Host "[SUCCESS] Connected to: $($context.Subscription.Name)" -ForegroundColor Green

    # 3. Detect Public IP
    Write-Host "[LOG] Step 3: Detecting your current Public IP address..." -ForegroundColor Gray
    $publicIp = (Invoke-RestMethod -Uri "https://api.ipify.org").Trim()
    $sourceAddressPrefix = "$($publicIp)/32"
    Write-Host "[INFO] Your IP address is: $publicIp (using $sourceAddressPrefix for JIT)" -ForegroundColor Cyan

    # 4. Get VM details
    Write-Host "[LOG] Step 4: Fetching details for VM '$VMName' in group '$ResourceGroupName'..." -ForegroundColor Gray
    $vm = Get-AzVM -ResourceGroupName $ResourceGroupName -Name $VMName -Status -ErrorAction Stop
    $vmStatus = ($vm.Statuses | Where-Object { $_.Code -like "PowerState*" }).DisplayStatus
    $location = $vm.Location.Replace(' ', '').ToLower()
    
    Write-Host "[INFO] VM Status: $vmStatus" -ForegroundColor Cyan
    Write-Host "[INFO] VM Location: $location" -ForegroundColor Cyan

    if ($vmStatus -eq "VM running") {
        
        # 5. Prepare JIT Request
        Write-Host "[LOG] Step 5: Generating Access Token and JIT request body..." -ForegroundColor Gray
        $endTime = (Get-Date).AddHours($DurationHours).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffffffZ")
        $token = (Get-AzAccessToken -ResourceUrl "https://management.azure.com").Token
        $headers = @{
            "Authorization" = "Bearer $token"
            "Content-Type" = "application/json"
        }

        $body = @{
            virtualMachines = @(
                @{
                    id = $vm.Id
                    ports = @(
                        @{
                            number = $Port
                            allowedSourceAddressPrefix = $sourceAddressPrefix
                            endTimeUtc = $endTime
                        }
                    )
                }
            )
        } | ConvertTo-Json -Depth 5

        # 6. Execute JIT Request
        $url = "https://management.azure.com/subscriptions/$SubscriptionId/resourceGroups/$ResourceGroupName/providers/Microsoft.Security/locations/$location/jitNetworkAccessPolicies/default/initiate?api-version=2020-01-01"
        
        Write-Host "[LOG] Step 6: Sending POST request to Microsoft.Security API..." -ForegroundColor Gray
        Write-Host "[INFO] Requesting access for port $Port for the next $DurationHours hour(s)..." -ForegroundColor Yellow
        
        $response = Invoke-RestMethod -Uri $url -Method Post -Headers $headers -Body $body
        
        # 7. Success Output
        Write-Host "`n✅ JIT ACCESS GRANTED SUCCESSFULLY!" -ForegroundColor Green
        Write-Host "------------------------------------------------"
        Write-Host " Target VM:    $VMName"
        Write-Host " Access Port:  $Port"
        Write-Host " Allowed IP:   $sourceAddressPrefix"
        Write-Host " Expires At:   $endTime (UTC)"
        Write-Host "------------------------------------------------"
    }
    else {
        Write-Host "`n❌ FAILED: VM '$VMName' must be running to request JIT access." -ForegroundColor Red
        Write-Host "[INFO] Current Status: $vmStatus" -ForegroundColor Yellow
    }
}
catch {
    Write-Host "`n❌ CRITICAL ERROR OCCURRED:" -ForegroundColor Red
    Write-Host "Message: $($_.Exception.Message)" -ForegroundColor Red
    if ($_.ErrorDetails.Message) {
        Write-Host "Details: $($_.ErrorDetails.Message)" -ForegroundColor Yellow
    }
}
finally {
    Write-Host "`n[LOG] Script execution finished." -ForegroundColor Gray
}