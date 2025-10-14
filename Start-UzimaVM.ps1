param(
    [Parameter(Mandatory=$false)]
    [String] $WebhookData
)

$ErrorActionPreference = "Stop"

# Get the VM
$vm = Get-AzVM -ResourceGroupName "cdiouzima" -Name "uzima" -Status

# Check if VM is already running
if ($vm.Statuses[1].Code -eq "PowerState/running") {
    Write-Output "VM is already running"
} else {
    # Start the VM
    Start-AzVM -ResourceGroupName "cdiouzima" -Name "uzima"
    Write-Output "Starting VM..."
}
