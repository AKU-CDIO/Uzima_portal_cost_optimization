import logging
import os
import sys
import subprocess
import time
import traceback
import json
import re
from datetime import datetime, timezone

# =============================================================================
# LOGGING
# =============================================================================
logger = logging.getLogger('UzimaVMAutoShutdown')
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S,000')
if not logger.handlers:
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

# =============================================================================
# DEPENDENCIES
# =============================================================================
def install_package(package):
    logger.info(f"🔧 Attempting to install package: {package}")
    PIP_TIMEOUT = 300
    try:
        result = subprocess.run(
            [sys.executable, "-m", "pip", "install", package],
            check=True, capture_output=True, text=True, timeout=PIP_TIMEOUT
        )
        logger.info(f"✅ Successfully installed {package}")
        logger.debug(f"Installation output: {result.stdout}")
        return True
    except subprocess.TimeoutExpired:
        logger.error(f"❌ Failed to install {package}: timed out after {PIP_TIMEOUT}s.")
        return False
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ Failed to install {package}")
        logger.error(f"stderr: {e.stderr}")
        logger.error(f"stdout: {e.stdout}")
        return False
    except Exception as e:
        logger.error(f"❌ Unexpected error installing {package}: {str(e)}")
        logger.error(traceback.format_exc())
        return False

def check_package_installed(package_name):
    try:
        module = __import__(package_name.replace('-', '_'))
        version = getattr(module, '__version__', 'unknown')
        logger.info(f"📦 Found {package_name} version {version}")
        return True
    except ImportError:
        logger.warning(f"⚠️  {package_name} is not installed")
        return False

# =============================================================================
# CONFIG
# =============================================================================
def load_config():
    config = {
        "VM_NAME": os.getenv('VM_NAME', 'uzima-copied'),
        "RESOURCE_GROUP": os.getenv('RESOURCE_GROUP', 'CDIOUZIMA'),
        "SUBSCRIPTION_ID": os.getenv('AZURE_SUBSCRIPTION_ID', 'a5d4ffbe-d287-4dd1-86c9-f1214fe751d6'),

        # Policy thresholds
        "INACTIVITY_THRESHOLD": int(os.getenv('INACTIVITY_THRESHOLD', '3600')),  # seconds
        "IDLE_SESSION_THRESHOLD_MINUTES": int(os.getenv('IDLE_SESSION_THRESHOLD_MINUTES', '60')),  # minutes

        # NEW: minimum uptime (seconds) required before any shutdown
        "MIN_UPTIME_SECONDS": int(os.getenv('MIN_UPTIME_SECONDS', '1800')),  # 30 minutes

        # State file (used when RDP is live; otherwise we prefer last disconnect)
        "LAST_ACTIVITY_FILE": os.getenv('LAST_ACTIVITY_FILE', r"C:\ProgramData\UzimaAutoShutdown\last_activity.txt"),

        # Blob SAS URL
        "BLOB_SAS_URL": os.getenv(
            'BLOB_SAS_URL',
            "https://uzimavmcontrol.blob.core.windows.net/uzima-vm-control-data?sv=2024-11-04&ss=bfqt&srt=sco&sp=rwdlacupitfx&se=2030-10-27T15:56:07Z&st=2025-10-27T07:41:07Z&spr=https,http&sig=5sS7aN30M30QrF5yzR2N%2F0o%2FapctDL3VCzvUWWjkof4%3D"
        ),

        # Safe testing
        "DRY_RUN": os.getenv('DRY_RUN', '1') in ('1', 'true', 'True', 'YES', 'yes'),
    }

    config["VM_RESOURCE_ID"] = (
        f"/subscriptions/{config['SUBSCRIPTION_ID']}/"
        f"resourceGroups/{config['RESOURCE_GROUP']}/"
        f"providers/Microsoft.Compute/virtualMachines/{config['VM_NAME']}"
    )

    logger.info("⚙️  Loaded configuration:")
    for key, value in config.items():
        if any(s in key for s in ("KEY", "SECRET", "PASSWORD", "SAS")):
            logger.info(f"  - {key}: {'*' * 8} (hidden)")
        else:
            logger.info(f"  - {key}: {value}")

    return config

SYSTEM_ACCOUNT_TOKENS = {"SYSTEM", "LOCAL SERVICE", "NETWORK SERVICE", "DWM-", "UMFD-"}
SYSTEM_ACCOUNT_PREFIXES = ("NT AUTHORITY\\", "NT SERVICE\\", "Window Manager\\")

def is_system_like_user(user: str) -> bool:
    if not user:
        return True
    u = user.strip()
    if u.upper() in SYSTEM_ACCOUNT_TOKENS:
        return True
    for p in SYSTEM_ACCOUNT_PREFIXES:
        if u.upper().startswith(p.upper()):
            return True
    for t in SYSTEM_ACCOUNT_TOKENS:
        if t.endswith('-') and u.upper().startswith(t.upper()):
            return True
    return False

def simple_user(user: str) -> str:
    if not user:
        return ""
    if "\\" in user:
        return user.split("\\")[-1]
    return user

# =============================================================================
# AUTH
# =============================================================================
def get_azure_credential():
    try:
        logger.info("🔑 Attempting to authenticate with Azure...")
        from azure.identity import DefaultAzureCredential, ChainedTokenCredential, ManagedIdentityCredential
        credential = ChainedTokenCredential(
            ManagedIdentityCredential(),
            DefaultAzureCredential(exclude_managed_identity_credential=True)
        )
        credential.get_token("https://management.azure.com/.default")
        logger.info("✅ Successfully authenticated with Azure")
        return credential
    except Exception:
        logger.error("❌ Failed to authenticate with Azure")
        logger.error(traceback.format_exc())
        raise

# =============================================================================
# RUN COMMAND
# =============================================================================
def execute_run_command_wait(compute_client, script, wait_for_completion=True, log_on_error=True):
    try:
        from azure.core.exceptions import DecodeError
        poller = compute_client.virtual_machines.begin_run_command(
            CONFIG['RESOURCE_GROUP'],
            CONFIG['VM_NAME'],
            {'command_id': 'RunPowerShellScript', 'script': [script], 'parameters': []}
        )
        if wait_for_completion:
            result = poller.result()
            if result and result.value:
                output = result.value[0].message
                return output.strip()
            return ""
        else:
            poller.wait()
            return poller
    except DecodeError:
        if log_on_error:
            logger.error("❌ Decode Error: Request blocked by network/security policy.")
            logger.error(traceback.format_exc())
        raise
    except Exception as e:
        if log_on_error:
            logger.error(f"❌ Error executing Run Command: {str(e)}")
            logger.error(traceback.format_exc())
        raise

# =============================================================================
# PARSE HELPERS
# =============================================================================
def _extract_last_json_object(text: str):
    if not text:
        return None
    s = text.strip()
    try:
        return json.dumps(json.loads(s))
    except Exception:
        pass

    start_idx = None
    depth = 0
    last_object = None
    for i, ch in enumerate(text):
        if ch == '{':
            if depth == 0:
                start_idx = i
            depth += 1
        elif ch == '}':
            if depth > 0:
                depth -= 1
                if depth == 0 and start_idx is not None:
                    last_object = text[start_idx:i + 1]

    if last_object:
        try:
            json.loads(last_object)
            return last_object
        except Exception:
            pass

    first = text.find('{')
    last = text.rfind('}')
    if first != -1 and last != -1 and last > first:
        candidate = text[first:last + 1]
        try:
            json.loads(candidate)
            return candidate
        except Exception:
            return None
    return None

def _parse_json_from_ps(output_text: str):
    if not output_text:
        return None
    try:
        return json.loads(output_text.strip())
    except Exception:
        pass

    lines = [l for l in output_text.splitlines() if l.strip()]
    if "##JSON##" in lines:
        try:
            idx = lines.index("##JSON##")
            json_str = "\n".join(lines[idx + 1:])
            return json.loads(json_str)
        except Exception:
            pass

    candidate = _extract_last_json_object(output_text)
    if candidate:
        try:
            return json.loads(candidate)
        except Exception:
            pass

    logger.warning("⚠️  Failed to parse JSON from PowerShell output.")
    logger.debug(output_text)
    return None

# =============================================================================
# COLLECTOR
# =============================================================================
def check_user_activity(compute_client):
    """
    Returns:
      success, details[], raw_sessions_all, raw_processes, raw_text, last_disconnect_iso, human_active_users[], human_active_sessions[], rdp_live (bool)
    """
    log_details = []
    try:
        logger.info(f"🔍 Checking for user sessions and key processes (Idle Threshold: {CONFIG['IDLE_SESSION_THRESHOLD_MINUTES']} min)...")

        SCRIPT = r'''
$ErrorActionPreference = 'SilentlyContinue'
$ProgressPreference    = 'SilentlyContinue'
$InformationPreference = 'SilentlyContinue'

$IdleThresholdMinutes = __IDLE__

function Get-IdleMinutes {
  param([string]$IdleTimeStr)
  if (-not $IdleTimeStr) { return 0 }
  if ($IdleTimeStr -eq 'none') { return 0 }
  if ($IdleTimeStr -match '(\d+)\s*\+') { return $IdleThresholdMinutes + 1 }
  if ($IdleTimeStr -match '^\d+:\d+$') { $p=$IdleTimeStr.Split(':'); return ([int]$p[0]*60)+[int]$p[1] }
  if ($IdleTimeStr -match '^\d+$') { return [int]$IdleTimeStr }
  return 0
}
function Get-SimpleUser { param([string]$u) if (-not $u) {return ''} if ($u.Contains('\')) {$u.Split('\')[-1]} else {$u} }

# A) Sessions via qwinsta + quser
$qw = & qwinsta
$qwLines = @(); if ($qw) { $qwLines = ($qw -split "`n") | Select-Object -Skip 1 }
$winSessions = @()
foreach ($l in $qwLines) {
  if (-not $l.Trim()) { continue }
  $line = ($l -replace '^\s+', '') -replace '\s{2,}', ' '
  $parts = $line.Split(' ')
  if ($parts.Count -lt 3) { continue }
  $SESSIONNAME = $parts[0]; $USERNAME=''; $ID=''; $STATE=''
  foreach ($p in $parts) { if ($p -match '^\d+$') { $ID=$p; break } }
  if (-not $ID) { continue }
  $idIndex = [Array]::IndexOf($parts, $ID)
  if ($idIndex -ge 0 -and $idIndex+1 -lt $parts.Count) { $STATE = $parts[$idIndex+1] }
  if ($idIndex -ge 2) { $USERNAME = ($parts[1..($idIndex-1)] -join ' ').Trim() }
  $winSessions += [pscustomobject]@{ SessionId=$ID; SessionName=$SESSIONNAME; Username=$USERNAME; State=$STATE }
}
$winById = @{}; foreach ($ws in $winSessions) { $winById[$ws.SessionId]=$ws }

$qu = & quser
$quLines = @(); if ($qu) { $quLines = ($qu -split "`n") | Select-Object -Skip 1 }
$usrSessions = @()
foreach ($l in $quLines) {
  if (-not $l.Trim()) { continue }
  $ln = $l.Replace('*',' ')
  $line = ($ln -replace '^\s+', '') -replace '\s{2,}', ' '
  $parts = $line.Split(' ')
  if ($parts.Count -lt 3) { continue }
  $ID=''; foreach ($p in $parts) { if ($p -match '^\d+$') { $ID=$p; break } }
  if (-not $ID) { continue }
  $states=@('Active','Disc','Disconnected','Conn','Listen','Down','Idle')
  $stateIdx=-1; for ($i=0; $i -lt $parts.Count; $i++) { if ($states -contains $parts[$i]) { $stateIdx=$i; break } }
  $idleTok='0'; if ($stateIdx -ge 0 -and $stateIdx+1 -lt $parts.Count) { $idleTok=$parts[$stateIdx+1] } elseif ($parts.Count -ge 2) { $idleTok=$parts[$parts.Count-2] }
  $idIndex=[Array]::IndexOf($parts,$ID); $USERNAME=''; if ($idIndex -ge 2) { $USERNAME=($parts[1..($idIndex-1)] -join ' ').Trim() }
  $usrSessions += [pscustomobject]@{ SessionId=$ID; Username=$USERNAME; IdleTime=$idleTok }
}
$usrById=@{}; foreach ($us in $usrSessions) { $usrById[$us.SessionId]=$us }

function Test-IsSystemSess { param($name,$id,$state,$user)
  if (-not $user) { return $true }
  if ($state -eq 'Listen') { return $true }
  if ($name -match '^(console|services)$') { return $true }
  if (@('0','65536','65537') -contains "$id") { return $true }
  return $false
}

$sessionRecords=@(); $joinedIds=New-Object 'System.Collections.Generic.HashSet[string]'
foreach ($id in $winById.Keys) {
  $w=$winById[$id]; $u=$null; if ($usrById.ContainsKey($id)) { $u=$usrById[$id] }
  $username = if ($w.Username) { $w.Username } elseif ($u) { $u.Username } else { '' }
  $idleMin = if ($u) { Get-IdleMinutes -IdleTimeStr $u.IdleTime } else { 0 }
  $isSystem = Test-IsSystemSess $w.SessionName $id $w.State $username
  $sessionRecords += [pscustomobject]@{
    user        = $username
    simpleUser  = (Get-SimpleUser $username)
    sessionId   = $id
    sessionName = $w.SessionName
    state       = $w.State
    idleMinutes = $idleMin
    hasUser     = ([string]::IsNullOrWhiteSpace($username) -eq $false)
    isSystem    = $isSystem
  }
  [void]$joinedIds.Add($id)
}
foreach ($id in $usrById.Keys) {
  if ($joinedIds.Contains($id)) { continue }
  $u=$usrById[$id]; $idleMin=Get-IdleMinutes -IdleTimeStr $u.IdleTime
  $isSystem = Test-IsSystemSess '' $id '' $u.Username
  $sessionRecords += [pscustomobject]@{
    user=$u.Username; simpleUser=(Get-SimpleUser $u.Username); sessionId=$id; sessionName=''; state=''
    idleMinutes=$idleMin; hasUser=([string]::IsNullOrWhiteSpace($u.Username) -eq $false); isSystem=$isSystem
  }
}

# Human-active (Active/Conn/Disc under idle threshold)
$humanActiveSessions = @($sessionRecords | Where-Object {
  $_.hasUser -and -not $_.isSystem -and $_.state -in @('Active','Conn','Disc') -and $_.idleMinutes -lt __IDLE__
})
$humanUsers = @($humanActiveSessions | ForEach-Object { $_.simpleUser } | Select-Object -Unique)

# B) Processes (script-aware + CPU sampling) — EXTENDED
$scriptExtensions = @(
  '.r','.R','.rmd','.Rmd','.qmd','.Qmd','.py','.ipynb','.ps1','.bat','.cmd','.vbs','.js','.mjs','.cjs',
  '.ts','.tsx','.sql','.m','.sas','.do','.jl','.java','.jar','.gradle','.groovy','.scala','.rb','.pl',
  '.php','.psm1','.psd1','.psd','.ps1xml','.fsx','.fs','.go','.sh','.psh','.q','.kdb','.qvs','.sparql'
)
$runnerNames = @(
  'python','pythonw','py','pyw','node','deno','bun',
  'rscript','rterm','rgui','rsession','rstudio',
  'jupyter','ipython','papermill',
  'powershell','pwsh','cmd','cscript','wscript',
  'java','javac','gradle','gradlew','mvn','mvnw',
  'dotnet','msbuild','vstest',
  'go','gcc','g++','clang','cl',
  'ruby','perl','php',
  'npm','yarn','pnpm','pip','pip3','conda','mamba',
  'airflow','celery','spark-submit','hadoop'
)

function Test-IsScriptCommandLine { param([string]$cmd, [string]$name)
  if (-not $cmd) { return $false }
  $lc=$cmd.ToLowerInvariant()
  foreach ($ext in $scriptExtensions) { if ($lc.Contains($ext.ToLowerInvariant())) { return $true } }
  if ($name -match '^(powershell|pwsh)$' -and ($lc -match '\s-File\s+|\.ps1\b')) { return $true }
  if ($name -eq 'cmd' -and ($lc -match '\s/c\s+.*(\.ps1|\.bat|\.cmd|\.js|\.vbs|\.py|\.r|\.psm1|\.psd1)\b')) { return $true }
  if ($name -eq 'node' -and ($lc -match '\.(m?js|cjs)\b|-m\s+\S+')) { return $true }
  if (($name -in @('cscript','wscript')) -and ($lc -match '\.(js|vbs)\b')) { return $true }
  if ($name -in @('python','pythonw','py','pyw','jupyter','ipython')) { return $true }
  if ($name -in @('rscript','rterm','rstudio','rsession')) { return $true }
  if ($name -in @('java','javac','gradle','gradlew','mvn','mvnw','dotnet','go','ruby','perl','php','deno','bun')) { return $true }
  return $false
}

$procRecords = @()
try {
  $gp = Get-Process -IncludeUserName -ErrorAction Stop | Where-Object { $_.SessionId -ne 0 -and $_.UserName }
  $cpu1=@{}; foreach ($p in $gp) { $cpu1["$($p.Id)"] = [double]$p.CPU }
  Start-Sleep -Seconds 3
  $all2 = Get-Process -ErrorAction SilentlyContinue
  $map2=@{}; foreach ($p2 in $all2) { $map2["$($p2.Id)"] = $p2 }

  $pids = $gp.Id | Select-Object -Unique
  $cmdMap = @{}
  if ($pids -and $pids.Count -gt 0) {
    $wmi = Get-CimInstance Win32_Process | Where-Object { $pids -contains $_.ProcessId }
    foreach ($w in $wmi) { $cmdMap["$($w.ProcessId)"] = $w.CommandLine }
  }

  foreach ($p in $gp) {
    $u = $p.UserName
    if ([string]::IsNullOrWhiteSpace($u)) { continue }
    $name = $p.Name.ToLowerInvariant()
    $cmd = ''; if ($cmdMap.ContainsKey("$($p.Id)")) { $cmd=$cmdMap["$($p.Id)"] }
    $cmdShort = if ($cmd) { $cmd.Substring(0, [Math]::Min(500, $cmd.Length)) } else { '' }
    $cpuBefore = if ($cpu1.ContainsKey("$($p.Id)")) { [double]$cpu1["$($p.Id)"] } else { 0.0 }
    $cpuAfter  = if ($map2.ContainsKey("$($p.Id)")) { [double]$map2["$($p.Id)"].CPU } else { $cpuBefore }
    $cpuDelta  = [double]($cpuAfter - $cpuBefore)
    $isRunner = ($runnerNames -contains $name)
    $isScriptLine = Test-IsScriptCommandLine -cmd $cmd -name $name
    $isBusy = ($cpuDelta -gt 0.01)
    $isLongRunningRunner = ($isRunner -and $cpuAfter -ge 1.0)
    $qualifies = ($isScriptLine -or $isBusy -or $isLongRunningRunner)
    $procRecords += [pscustomobject]@{
      user=$u; simpleUser=(Get-SimpleUser $u); process=$p.Name; sessionId=$p.SessionId; command=$cmdShort
      isRunner=$isRunner; isScriptLine=$isScriptLine; cpuDelta=$cpuDelta; cpuTotal=$cpuAfter
      isBusy=$isBusy; isLongRunningRunner=$isLongRunningRunner; qualifies=$qualifies
    }
  }
} catch {
  $procRecords = Get-CimInstance Win32_Process | Where-Object { $_.SessionId -ne 0 } | ForEach-Object {
    $user=''; try { $owner=$_.GetOwner(); $user="$($owner.Domain)\$($owner.User)" } catch {}
    if ($user) {
      $cmd=""; if ($_.CommandLine) { $cmd=$_.CommandLine.Substring(0, [System.Math]::Min(500, $_.CommandLine.Length)) }
      $name=$_.Name.ToLowerInvariant()
      $isRunner = ($runnerNames -contains $name)
      $isScriptLine = Test-IsScriptCommandLine -cmd $cmd -name $name
      $qualifies = ($isScriptLine -or $isRunner)
      [pscustomobject]@{
        user=$user; simpleUser=(Get-SimpleUser $user); process=$_.Name; sessionId=$_.SessionId; command=$cmd
        isRunner=$isRunner; isScriptLine=$isScriptLine; cpuDelta=0.0; cpuTotal=0.0
        isBusy=$false; isLongRunningRunner=$false; qualifies=$qualifies
      }
    }
  }
}

# C) Derive RDP "live" from sessions and processes
$rdpLive = $false
$rdpLiveSessions = @()
foreach ($s in $sessionRecords) {
  $nm = ($s.sessionName | Out-String).Trim().ToLower()
  if ($nm -like 'rdp-*' -and $s.state -in @('Active','Conn')) {
    $rdpLive = $true
    $rdpLiveSessions += $s
  }
}
if (-not $rdpLive) {
  foreach ($p in $procRecords) {
    $pn = ($p.process | Out-String).Trim().ToLower()
    if ($pn -in @('rdpclip','rdpinput')) {
      $rdpLive = $true
      break
    }
  }
}

# D) Last RDP disconnect (ID 24)
$lastDiscUtc = $null
$lastDiscUser = ''
try {
  $ev = Get-WinEvent -FilterHashtable @{LogName='Microsoft-Windows-TerminalServices-LocalSessionManager/Operational'; Id=@(21,24,25)} -MaxEvents 800 |
        Select-Object TimeCreated, Id, Message | Sort-Object TimeCreated
  $events = @()
  foreach ($e in $ev) {
    $u=''; $sid=''
    if ($e.Message -match 'User:\s*(?<u>[^\r\n]+)') { $u=$Matches['u'].Trim() }
    if ($e.Message -match 'Session ID:\s*(?<sid>\d+)') { $sid=$Matches['sid'].Trim() }
    if (-not $u) { continue }
    $events += [pscustomobject]@{ t=$e.TimeCreated; id=$e.Id; user=$u; sid=$sid }
  }
  $byKey = $events | Group-Object { "$($_.user)|$($_.sid)" }
  $candidates = @()
  foreach ($g in $byKey) {
    $arr = $g.Group | Sort-Object t
    for ($i=0; $i -lt $arr.Count; $i++) {
      if ($arr[$i].id -eq 24) {
        $hasReconnect = $false
        for ($j=$i+1; $j -lt $arr.Count; $j++) {
          if ($arr[$j].id -eq 25) { $hasReconnect=$true; break }
        }
        if (-not $hasReconnect) {
          $candidates += $arr[$i]
        }
      }
    }
  }
  if ($candidates.Count -gt 0) {
    $best = $candidates | Sort-Object t -Descending | Select-Object -First 1
    $lastDiscUtc = $best.t
    $lastDiscUser = $best.user
  }
} catch {}

$lastDiscIso = $null
if ($lastDiscUtc) { $lastDiscIso = ([DateTime]::SpecifyKind($lastDiscUtc, 'Local')).ToUniversalTime().ToString('o') }

# E) Output JSON
$result = [pscustomobject]@{
  sessions_all            = $sessionRecords;
  processes               = $procRecords;
  human_active_sessions   = $humanActiveSessions;
  human_active_users      = $humanUsers;
  rdp_live                = [bool]$rdpLive;
  last_disconnect_utc     = $lastDiscIso;
  last_disconnect_user    = $lastDiscUser
}
$ResultJson = $result | ConvertTo-Json -Depth 8 -Compress
$ResultJson
'''
        ps_script = SCRIPT.replace("__IDLE__", str(CONFIG['IDLE_SESSION_THRESHOLD_MINUTES']))
        output = execute_run_command_wait(compute_client, ps_script)

        if output:
            logger.info(f"🧪 RAW_PS_JSON (first 2000 chars): {output[:2000]}")

        data = _parse_json_from_ps(output)
        log_details.append({"type": "Collector_Status", "parsed": bool(data)})

        # Defaults
        raw_sessions_all = []
        raw_processes = []
        human_active_sessions = []
        human_active_users = []
        rdp_live = False
        last_disconnect_iso = None

        if data:
            raw_sessions_all = data.get("sessions_all") or []
            raw_processes = data.get("processes") or []
            human_active_sessions = data.get("human_active_sessions") or []
            human_active_users = data.get("human_active_users") or []
            rdp_live = bool(data.get("rdp_live", False))
            last_disconnect_iso = data.get("last_disconnect_utc") or None

            log_details.append({"type": "Collector_Data",
                                "sessions_count": len(raw_sessions_all),
                                "processes_count": len(raw_processes),
                                "human_active_sessions": len(human_active_sessions),
                                "rdp_live": rdp_live,
                                "last_disconnect_utc": last_disconnect_iso})
        else:
            logger.warning("⚠️  Could not parse JSON; will rely on raw-text fallbacks.")
        return True, log_details, raw_sessions_all, raw_processes, (output or ""), last_disconnect_iso, human_active_users, human_active_sessions, rdp_live

    except Exception as e:
        logger.error(f"❌ Error in collector: {str(e)}")
        logger.error(traceback.format_exc())
        return False, [{"type":"Collector_Error","detail":str(e)}], [], [], "", None, [], [], False

# =============================================================================
# LAST ACTIVITY FILE
# =============================================================================
def update_last_activity(compute_client):
    try:
        logger.info("⏱️  Updating last activity timestamp...")
        SCRIPT = r"""
$filePath = "__FILE_PATH__"
$dirPath = Split-Path -Parent $filePath
if (-not (Test-Path $dirPath)) { New-Item -Path $dirPath -ItemType Directory | Out-Null }
$timestamp = [int][double](Get-Date (Get-Date).ToUniversalTime() -UFormat %s)
Set-Content -Path $filePath -Value $timestamp
"""
        update_script = SCRIPT.replace("__FILE_PATH__", CONFIG['LAST_ACTIVITY_FILE'])
        execute_run_command_wait(compute_client, update_script)
        logger.info("✅ Successfully updated last activity timestamp")
    except Exception:
        logger.error("❌ Error updating last activity")
        logger.error(traceback.format_exc())
        raise

def get_last_activity(compute_client):
    try:
        logger.info("⏱️  Retrieving last activity timestamp...")
        SCRIPT = r"""
$filePath = "__FILE_PATH__"
if (Test-Path $filePath) {
    Get-Content $filePath -Raw
} else {
    [int][double](Get-Date (Get-Date).ToUniversalTime() -UFormat %s)
}
"""
        read_script = SCRIPT.replace("__FILE_PATH__", CONFIG['LAST_ACTIVITY_FILE'])
        timestamp_str = execute_run_command_wait(compute_client, read_script, wait_for_completion=True, log_on_error=False)
        try:
            timestamp = float(timestamp_str)
            logger.info(f"📅 Last activity was at: {datetime.fromtimestamp(timestamp).isoformat()}")
            return timestamp
        except ValueError:
            logger.warning(f"⚠️  Invalid timestamp '{timestamp_str}'. Using current time (reset).")
            return time.time()
    except Exception:
        logger.error("❌ Error reading last activity")
        logger.error(traceback.format_exc())
        logger.warning("⚠️  Using current time as last activity (reset for safety)")
        return time.time()

# =============================================================================
# NEW: VM UPTIME (seconds)
# =============================================================================
def get_vm_uptime_seconds(compute_client) -> int:
    try:
        SCRIPT = r"""
$boot = (Get-CimInstance Win32_OperatingSystem).LastBootUpTime
$bootDT = [Management.ManagementDateTimeConverter]::ToDateTime($boot)
$uptime = [int]([DateTime]::Now - $bootDT).TotalSeconds
$uptime
"""
        out = execute_run_command_wait(compute_client, SCRIPT, wait_for_completion=True, log_on_error=False)
        return int(float(out.strip()))
    except Exception:
        logger.warning("⚠️  Could not determine VM uptime; defaulting to large value (won't block shutdown).")
        return 10**9  # fail-open so we don't inadvertently block shutdowns

# =============================================================================
# VM MANAGEMENT
# =============================================================================
def stop_vm(compute_client):
    try:
        if CONFIG.get("DRY_RUN", True):
            logger.warning("🧪 DRY_RUN is ON: Skipping actual VM deallocation.")
            return True
        logger.warning(f"🛑 Attempting to deallocate VM: {CONFIG['VM_NAME']}")
        async_vm_stop = compute_client.virtual_machines.begin_deallocate(
            resource_group_name=CONFIG['RESOURCE_GROUP'],
            vm_name=CONFIG['VM_NAME']
        )
        async_vm_stop.wait()
        if async_vm_stop.status() == 'Succeeded':
            logger.info(f"✅ Successfully deallocated VM: {CONFIG['VM_NAME']}")
            return True
        else:
            logger.error(f"❌ Failed to stop VM: {CONFIG['VM_NAME']} (Status: {async_vm_stop.status()})")
            return False
    except Exception:
        logger.error("❌ Error stopping VM")
        logger.error(traceback.format_exc())
        return False

# =============================================================================
# RAW TEXT RDP FALLBACK (PRECISE)
# =============================================================================
def _rdp_live_from_raw(raw_text: str):
    if not raw_text:
        return False, []
    pattern = re.compile(
        r'\{[^{}]*"sessionName"\s*:\s*"(?P<sn>rdp-[^"]+)"[^{}]*"state"\s*:\s*"(?P<state>Active|Conn)"[^{}]*\}',
        re.IGNORECASE
    )
    live = []
    for m in pattern.finditer(raw_text):
        block = m.group(0)
        session_name = m.group('sn')
        state = m.group('state').title()
        sid_m = re.search(r'"sessionId"\s*:\s*"?(?P<sid>\d+)"?', block, re.IGNORECASE)
        session_id = sid_m.group('sid') if sid_m else ""
        user = ""
        su_m = re.search(r'"simpleUser"\s*:\s*"(?P<u>[^"]+)"', block, re.IGNORECASE)
        if su_m:
            user = su_m.group('u')
        else:
            u_m = re.search(r'"user"\s*:\s*"(?P<u>[^"]+)"', block, re.IGNORECASE)
            if u_m:
                user = u_m.group('u')
        live.append({
            "sessionName": session_name, "sessionId": session_id,
            "state": state, "user": user, "source": "raw-text"
        })
    return (len(live) > 0), live

# (… your raw/script helpers & normalization remain unchanged …)

# =============================================================================
# MAIN
# =============================================================================
def main():
    logger.info("\n" + "="*40)
    logger.info("🚀 Starting Uzima VM Auto-Shutdown Runbook")
    logger.info("="*40)
    try:
        from azure.mgmt.compute import ComputeManagementClient

        credential = get_azure_credential()
        compute_client = ComputeManagementClient(credential=credential, subscription_id=CONFIG['SUBSCRIPTION_ID'])

        logger.info("\n" + "="*40)
        logger.info("🔍 Running Primary User Activity Check...")
        logger.info("="*40)

        ok, details, sessions_all, processes_all, raw_text, last_disc_iso, human_users, human_sessions, rdp_live = check_user_activity(compute_client)

        # Guard 1 …
        # (unchanged logic above)

        # Inactivity base:
        last_activity_ts_file = get_last_activity(compute_client)
        now_epoch = time.time()
        base_ts = last_activity_ts_file
        if not rdp_live and last_disc_iso:
            try:
                base_ts = datetime.fromisoformat(last_disc_iso.replace('Z', '+00:00')).timestamp()
            except Exception:
                base_ts = last_activity_ts_file
        inactive_secs = now_epoch - base_ts

        # NEW: VM uptime
        uptime_secs = get_vm_uptime_seconds(compute_client)

        # Report guards (extended)
        logger.info("\n" + "="*40)
        logger.info("Decision Guards and Timers")
        logger.info("="*40)
        # (existing logging …)
        logger.info(f"⏲️ VM uptime: {uptime_secs}s (min_required={CONFIG['MIN_UPTIME_SECONDS']}s)")
        logger.info(f"Timer: inactive_secs={int(inactive_secs)}s (threshold={CONFIG['INACTIVITY_THRESHOLD']}s)")
        logger.info("="*40)

        guard_snapshot = {
            # (existing fields …)
            "uptime_secs": int(uptime_secs),
            "min_uptime_secs": int(CONFIG['MIN_UPTIME_SECONDS']),
        }

        from azure.storage.blob import BlobClient

        def upload_log():
            try:
                base_url_no_sas, sas_token = CONFIG['BLOB_SAS_URL'].split('?', 1)
                blob_client = BlobClient.from_blob_url(
                    blob_url=f"{base_url_no_sas.rstrip('/')}/{CONFIG['VM_NAME']}/activity_log_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}.json?{sas_token}",
                    credential=None
                )
                payload = {
                    "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                    "vm_name": CONFIG['VM_NAME'],
                    "guards": guard_snapshot,
                    "sessions_all": sessions_all or [],
                    "processes_all": processes_all or [],
                    "raw_excerpt": (raw_text or "")[:2000]
                }
                blob_client.upload_blob(json.dumps(payload, indent=2).encode("utf-8"), overwrite=True)
                logger.info("✅ Activity log uploaded to Blob.")
            except Exception as e:
                logger.error(f"❌ Failed to upload activity log: {e}")

        # ===== Decision flow (unchanged except uptime gate) =====
        if rdp_live:
            upload_log()
            update_last_activity(compute_client)
            return "Live RDP present; VM will not be shut down."

        if (len(human_sessions or []) > 0) or _any_user_process(processes_all):
            active_scripts_for_humans = _active_scripts_for_users(processes_all, human_users)
            if len(active_scripts_for_humans) > 0:
                upload_log()
                update_last_activity(compute_client)
                return "Active user scripts/processes detected; VM will not be shut down."
            else:
                if inactive_secs > CONFIG['INACTIVITY_THRESHOLD']:
                    # NEW: uptime gate
                    if uptime_secs < CONFIG['MIN_UPTIME_SECONDS']:
                        upload_log()
                        return "Waiting: VM was started recently (<30 min); shutdown deferred."
                    upload_log()
                    if stop_vm(compute_client):
                        return "VM successfully shut down due to inactivity (no scripts among human users; last disconnect exceeded threshold)."
                    else:
                        return "Failed to shut down VM."
                else:
                    mins_left = max(0.0, (CONFIG['INACTIVITY_THRESHOLD'] - inactive_secs) / 60.0)
                    upload_log()
                    return f"Waiting: last disconnect threshold not yet reached (~{mins_left:.1f} min remaining)."

        if inactive_secs > CONFIG['INACTIVITY_THRESHOLD']:
            # NEW: uptime gate
            if uptime_secs < CONFIG['MIN_UPTIME_SECONDS']:
                upload_log()
                return "Waiting: VM was started recently (<30 min); shutdown deferred."
            upload_log()
            if stop_vm(compute_client):
                return "VM successfully shut down due to inactivity (no human activity; last disconnect exceeded threshold)."
            else:
                return "Failed to shut down VM."
        else:
            mins_left = max(0.0, (CONFIG['INACTIVITY_THRESHOLD'] - inactive_secs) / 60.0)
            upload_log()
            return f"Waiting: last disconnect threshold not yet reached (~{mins_left:.1f} min remaining)."

    except Exception:
        logger.error("❌ Critical error in main execution.")
        logger.error(traceback.format_exc())
        return "❌ Critical error in main execution."

# =============================================================================
# ENTRYPOINT
# =============================================================================
if __name__ == "__main__":
    try:
        logger.info("=" * 80)
        logger.info("🚀 Starting Uzima VM Auto-Shutdown Setup")
        logger.info("=" * 80)

        required_packages = [
            "azure-identity>=1.12.0",
            "azure-mgmt-compute>=23.1.0",
            "azure-core>=1.29.5",
            "msrest>=0.7.1",
            "azure-storage-blob>=12.18.0",
        ]
        for package in required_packages:
            pkg_name = package.split('>=')[0].split('==')[0]
            if not check_package_installed(pkg_name):
                if not install_package(package):
                    logger.error(f"❌ Critical: Failed to install required package: {package}")
                    sys.exit(1)

        logger.info("\n" + "="*40)
        logger.info("🔍 Importing Azure modules...")
        logger.info("="*40)
        from azure.identity import DefaultAzureCredential, ManagedIdentityCredential, ChainedTokenCredential
        from azure.mgmt.compute import ComputeManagementClient
        from azure.core.exceptions import AzureError, ResourceNotFoundError, DecodeError
        from azure.storage.blob import BlobClient
        logger.info("✅ All required modules imported successfully")

        global CONFIG
        CONFIG = load_config()
        CONFIG['DRY_RUN'] = True  # your setting

        result = main()

        logger.info("\n" + "="*40)
        logger.info("🏁 Runbook completed")
        logger.info("="*40)
        logger.info(f"Result: {result}")
        sys.exit(0)
    except Exception:
        logger.error("\n" + "="*40)
        logger.error("💥 Runbook failed")
        logger.error("="*40)
        logger.error(traceback.format_exc())
        sys.exit(1)
