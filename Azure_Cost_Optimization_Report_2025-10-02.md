### 3.1 High-Impact Changes (Savings: ~$3,429.39)

#### 3.1.1 SQL Database Optimization (Save $1,000.00)
- **Current Configuration**:
  - **uzima_db**: 10 DTUs ($1,205.57)
  - **hcw_fitbit_survey_db**: vCore model ($549.60)

- **Recommended Actions**:
  1. **Downgrade `uzima_db` from 10 DTUs to 5 DTUs**
     - **Cost Impact**: ~50% reduction ($1,205.57 → ~$600)
     - **Performance Impact**:
       - **CPU/Memory**: 50% reduction in resources
       - **Query Performance**:
         - Simple queries: Minimal impact
         - Complex queries: 20-30% slower response times
         - Concurrent users: May decrease from ~100 to ~50
       - **Mitigation**: Monitor with:
         ```sql
         -- Check DTU usage
         SELECT 
             end_time,
             (COUNT(*) * 100) / (SELECT COUNT(*) FROM sys.dm_os_performance_counters 
                                WHERE counter_name = 'CPU usage %') AS [CPU Percent]
         FROM sys.dm_os_performance_counters
         WHERE [object_name] LIKE '%Resource Pool Stats%'
         AND counter_name = 'CPU usage %'
         AND instance_name = 'default'
         GROUP BY end_time
         ORDER BY end_time DESC;
         ```

  2. **Migrate `hcw_fitbit_survey_db` to serverless**
     - **Cost Impact**: 60-70% reduction ($549.60 → ~$165-220)
     - **Performance Impact**:
       - Auto-pauses after 1-6 hours of inactivity
       - Cold start delay of 5-15 seconds after pause
       - Ideal for development/test environments with intermittent usage

#### 3.1.2 Virtual Machine Right-Sizing (Save $600.00)
- **Current**: D32a v4 (32 vCPUs, 128GB RAM, ~$1,263.31/month)
- **Recommended**: D8a v4 (8 vCPUs, 32GB RAM, ~$315.83/month)--consider Daisy project

- **Performance Impact**:
  - **CPU**: 75% reduction (32 → 8 vCPUs)
    - Impact: May affect CPU-intensive operations
    - Mitigation: Monitor CPU metrics; scale up if sustained >70%
  - **Memory**: 75% reduction (128GB → 32GB)
    - Impact: May affect memory-intensive applications
    - Mitigation: Check current usage; ensure 20% buffer for peaks

- **Auto-Start/Stop Implementation**:
  ```powershell
  # Manual Start (CLI)
  az vm start --resource-group cdiouzima --name uzima
  
  # Scheduled Auto-Start (Azure Automation)
  $schedule = New-AzAutomationSchedule -ResourceGroupName "cdiouzima" `
    -AutomationAccountName "UzimaAutomation" `
    -Name "WeekdayMorningStart" `
    -DaysOfWeek "Monday", "Tuesday", "Wednesday", "Thursday", "Friday" `
    -StartTime "07:00" -TimeZone "E. Africa Standard Time"
  ```
  - **Login Impact**: No effect on authentication; only powers on the VM
  - **Permissions**: Requires VM Contributor role or higher

#### 3.1.3 Firewall Replacement (Save $936.00)
- **Current**: Azure Firewall Standard ($936.00)
- **Recommended**: Network Security Groups (NSGs)

- **Security Feature Comparison**:
  | Feature | Azure Firewall | NSGs | Mitigation for NSG Gaps |
  |---------|---------------|------|-------------------------|
  | **Stateful Inspection** | Yes | No (stateless by default) | Use Application Security Groups (ASGs) |
  | **Application FQDN Filtering** | Yes | No | Use Azure Web Application Firewall |
  | **TLS Inspection** | Yes | No | Consider Azure Application Gateway |
  | **Threat Intelligence** | Yes | No | Enable Microsoft Defender for Cloud |
  | **Logging** | Advanced | Basic | Enable NSG Flow Logs |
  | **Cost** | $936/month | Free | N/A |

- **Migration Steps**:
  1. Document current firewall rules
  2. Create equivalent NSG rules
  3. Test in non-production first
  4. Implement monitoring:
     ```powershell
     # Enable NSG Flow Logs
     $workspace = Get-AzOperationalInsightsWorkspace -ResourceGroupName cdiouzima
     $storageAccount = Get-AzStorageAccount -ResourceGroupName cdiouzima -Name uzimalogs
     $nsg = Get-AzNetworkSecurityGroup -ResourceGroupName cdiouzima -Name "Uzima-NSG"
     
     Set-AzNetworkWatcherConfigFlowLog `
       -NetworkWatcherName "NetworkWatcher_northeurope" `
       -ResourceGroupName "NetworkWatcherRG" `
       -TargetResourceId $nsg.Id `
       -StorageAccountId $storageAccount.Id `
       -EnableFlowLog $true `
       -FormatType "JSON" `
       -FormatVersion 2 `
       -EnableTrafficAnalytics `
       -WorkspaceResourceId $workspace.ResourceId `
       -WorkspaceGUID $workspace.CustomerId
     ```

#### 3.1.4 Storage Optimization (Save $300.00)
- **Current**:
  - Hot RA-GRS: $154.76
  - Disks: $375.13
  - Snapshots: $22.03

- **Optimization Plan**:
  1. **Lifecycle Management**:
     - Move to Cool tier after 30 days
     - Archive after 90 days
     - Delete after 365 days
     ```powershell
     # Example lifecycle policy (JSON)
     $rules = @{
       rules = @(
         @{
           name = "coolDownTiering"
           enabled = $true
           type = "Lifecycle"
           definition = @{
             actions = @{
               baseBlob = @{
                 tierToCool = @{ daysAfterModificationGreaterThan = 30 }
                 tierToArchive = @{ daysAfterModificationGreaterThan = 90 }
                 delete = @{ daysAfterModificationGreaterThan = 365 }
               }
             }
             filters = @{
               blobTypes = @("blockBlob")
               prefixMatch = @("archive/", "logs/")
             }
           }
         }
       )
     }
     $rules | ConvertTo-Json -Depth 10 | Set-Content lifecycle_policy.json
     ```

  2. **Cleanup Unattached Disks**:
     ```powershell
     # List all unattached disks
     $unattachedDisks = Get-AzDisk | Where-Object { $_.DiskState -eq 'Unattached' }
     $unattachedDisks | Select-Object Name, DiskSizeGB, TimeCreated | Format-Table
     
     # Delete unattached disks (uncomment to execute)
     # $unattachedDisks | Remove-AzDisk -Force
     ```

  3. **Snapshot Management**:
     ```powershell
     # List all snapshots older than 30 days
     $cutoffDate = (Get-Date).AddDays(-30)
     $oldSnapshots = Get-AzSnapshot | Where-Object { $_.TimeCreated -lt $cutoffDate }
     $oldSnapshots | Select-Object Name, TimeCreated, DiskSizeGB | Format-Table
     
     # Delete old snapshots (uncomment to execute)
     # $oldSnapshots | Remove-AzSnapshot -Force
     ```