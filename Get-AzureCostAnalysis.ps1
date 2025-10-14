# Install required modules if not already installed
$modules = @("Az.Accounts", "Az.Consumption", "ImportExcel")
foreach ($module in $modules) {
    if (-not (Get-Module -ListAvailable -Name $module)) {
        Write-Host "Installing $module module..."
        Install-Module -Name $module -Force -AllowClobber -Scope CurrentUser
    }
}

# Import modules
Import-Module Az.Accounts
Import-Module Az.Consumption
Import-Module ImportExcel

# Connect to Azure (using existing az login context)
Connect-AzAccount -UseDeviceAuthentication

# Set time range for cost analysis (last 30 days)
$endDate = Get-Date
$startDate = $endDate.AddDays(-30)

# 1. Get cost data by resource using Azure CLI
Write-Host "Retrieving cost data by resource..."
$costData = az consumption usage list --start-date $startDate.ToString("yyyy-MM-dd") --end-date $endDate.ToString("yyyy-MM-dd") | ConvertFrom-Json

# Process cost data
$costByResource = $costData | Select-Object @{Name="Resource"; Expression={$_.instanceName}}, 
    @{Name="ResourceGroup"; Expression={$_.instanceId -split '/' | Where-Object { $_ -ne '' }[4] }},
    @{Name="MeterCategory"; Expression={$_.meterDetails.meterCategory}},
    @{Name="MeterSubCategory"; Expression={$_.meterDetails.meterSubCategory}},
    @{Name="Cost"; Expression={$_.pretaxCost}},
    @{Name="Currency"; Expression={$_.currency}}

# 2. Get time-based cost data
Write-Host "Retrieving time-based cost data..."
$timeData = $costData | Select-Object @{Name="Date"; Expression={$_.usageStart}},
    @{Name="Resource"; Expression={$_.instanceName}},
    @{Name="Cost"; Expression={$_.pretaxCost}},
    @{Name="Currency"; Expression={$_.currency}}

# 3. Get active status and run history
Write-Host "Retrieving resource status and run history..."
$activityLogs = az monitor activity-log list --start-time $startDate.ToString("yyyy-MM-ddT00:00:00Z") --end-time $endDate.ToString("yyyy-MM-ddT23:59:59Z") --status "Succeeded" | ConvertFrom-Json
$activityLogs = $activityLogs | Where-Object { $_.operationName.localizedValue -match "write" -or $_.operationName.localizedValue -match "action" } |
    Select-Object @{Name="Timestamp"; Expression={$_.eventTimestamp}},
        @{Name="ResourceGroup"; Expression={$_.resourceGroupName}},
        @{Name="Resource"; Expression={$_.resourceId -split '/' | Select-Object -Last 1}},
        @{Name="Operation"; Expression={$_.operationName.localizedValue}},
        @{Name="Status"; Expression={$_.statusCode}},
        @{Name="Caller"; Expression={$_.caller}}

# Create Excel file
$excelPath = "$PSScriptRoot\AzureCostAnalysis_$(Get-Date -Format 'yyyyMMdd_HHmmss').xlsx"

Write-Host "Generating Excel report at $excelPath ..."

# Export to Excel with multiple sheets
$costByResource | Export-Excel -Path $excelPath -WorksheetName "CostByResource" -AutoSize -TableName "CostByResource" -FreezeTopRow -BoldTopRow
$timeData | Export-Excel -Path $excelPath -WorksheetName "TimeBasedCosts" -AutoSize -TableName "TimeBasedCosts" -FreezeTopRow -BoldTopRow
$activityLogs | Export-Excel -Path $excelPath -WorksheetName "ResourceActivity" -AutoSize -TableName "ResourceActivity" -FreezeTopRow -BoldTopRow

Write-Host "Report generated successfully at: $excelPath"
Write-Host "Opening the report..."
Invoke-Item $excelPath