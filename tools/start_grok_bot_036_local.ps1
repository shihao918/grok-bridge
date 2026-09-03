[CmdletBinding()]
param(
    [string]$AppPath = (Join-Path $PSScriptRoot "..\.tmp_app_candidate_036\Grok Bot.exe"),
    [string]$GatewayUrl = "http://127.0.0.1:9000",
    [ValidateSet("codex", "responses", "ollama")]
    [string]$ModelBackend = "codex",
    [string]$CodexConfigPath = (Join-Path $HOME ".codex\config.toml"),
    [ValidateRange(5, 120)]
    [int]$StartupTimeoutSeconds = 25,
    [switch]$DryRun,
    [switch]$SkipBackendHealthCheck,
    [switch]$NoStartBackend,
    [switch]$NoRestartExisting,
    [switch]$NoRestartBackend,
    [switch]$AllowInsecureRemoteHttpProvider
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
$python = Join-Path $repoRoot ".venv\Scripts\python.exe"
$backendScript = Join-Path $repoRoot "backend_server.py"
$modelRuntime = Join-Path $repoRoot "model_runtime.py"
foreach ($requiredPath in @($python, $backendScript, $modelRuntime, $CodexConfigPath)) {
    if (-not (Test-Path -LiteralPath $requiredPath -PathType Leaf)) {
        throw "Required launcher input is missing: $requiredPath"
    }
}
$pythonExecutableOutput = @(& $python -c "import sys; print(sys.executable)" 2>&1)
if ($LASTEXITCODE -ne 0 -or $pythonExecutableOutput.Count -eq 0) {
    throw "Could not resolve the backend Python executable"
}
$backendPythonExecutable = [IO.Path]::GetFullPath([string]$pythonExecutableOutput[-1])
if (-not (Test-Path -LiteralPath $backendPythonExecutable -PathType Leaf)) {
    throw "Resolved backend Python executable is missing: $backendPythonExecutable"
}
$resolvedCodexConfig = [IO.Path]::GetFullPath((Resolve-Path -LiteralPath $CodexConfigPath).Path)
$resolvedApp = [IO.Path]::GetFullPath((Resolve-Path -LiteralPath $AppPath).Path)
$appItem = Get-Item -LiteralPath $resolvedApp
if ($appItem.PSIsContainer) {
    throw "AppPath must point to Grok Bot.exe: $resolvedApp"
}

try {
    $gateway = [Uri]$GatewayUrl
}
catch {
    throw "GatewayUrl is not a valid absolute URI: $GatewayUrl"
}

$loopbackHosts = @("127.0.0.1", "localhost", "::1")
if (-not $gateway.IsAbsoluteUri -or $gateway.Scheme -ne "http" -or $gateway.Host -notin $loopbackHosts) {
    throw "GatewayUrl must be an HTTP loopback address; refusing non-local gateway: $GatewayUrl"
}

$gatewayBase = $gateway.GetLeftPart([UriPartial]::Authority)
$healthUrl = "$gatewayBase/health"
$modelRuntimeUrl = "$gatewayBase/model-runtime"
$restartExisting = -not $NoRestartExisting

function Invoke-WithModelEnvironment {
    param([Parameter(Mandatory)][scriptblock]$Action)

    $names = @("GROK_MODEL_BACKEND", "GROK_CODEX_CONFIG_PATH", "GROK_ALLOW_INSECURE_REMOTE_HTTP")
    $previous = @{}
    foreach ($name in $names) {
        $previous[$name] = if (Test-Path "Env:$name") { Get-Item "Env:$name" | Select-Object -ExpandProperty Value } else { $null }
    }
    try {
        $env:GROK_MODEL_BACKEND = $ModelBackend
        $env:GROK_CODEX_CONFIG_PATH = $resolvedCodexConfig
        if ($AllowInsecureRemoteHttpProvider) {
            $env:GROK_ALLOW_INSECURE_REMOTE_HTTP = "1"
        }
        else {
            Remove-Item Env:GROK_ALLOW_INSECURE_REMOTE_HTTP -ErrorAction SilentlyContinue
        }
        & $Action
    }
    finally {
        foreach ($name in $names) {
            if ($null -ne $previous[$name]) {
                Set-Item -LiteralPath "Env:$name" -Value $previous[$name]
            }
            else {
                Remove-Item -LiteralPath "Env:$name" -ErrorAction SilentlyContinue
            }
        }
    }
}

function Get-RequestedModelBinding {
    $output = @(Invoke-WithModelEnvironment {
        & $python $modelRuntime --require-auth 2>&1
    })
    $exitCode = $LASTEXITCODE
    $text = ($output | ForEach-Object { [string]$_ }) -join "`n"
    if ($exitCode -ne 0) {
        throw "Model binding validation failed: $text"
    }
    try {
        return $text | ConvertFrom-Json
    }
    catch {
        throw "Model binding validation did not return JSON: $text"
    }
}

$modelBinding = Get-RequestedModelBinding

if ($DryRun) {
    [ordered]@{
        appPath = $resolvedApp
        gatewayUrl = $gatewayBase
        healthUrl = $healthUrl
        modelRuntimeUrl = $modelRuntimeUrl
        modelBinding = $modelBinding
        restartExisting = $restartExisting
        restartBackendOnMismatch = -not $NoRestartBackend
        startBackendIfMissing = -not $NoStartBackend
        allowInsecureRemoteHttpProvider = [bool]$AllowInsecureRemoteHttpProvider
        environment = [ordered]@{
            SAND_HOST_GATEWAY_URL = $gatewayBase
            GROK_MODEL_BACKEND = $ModelBackend
            GROK_CODEX_CONFIG_PATH = $resolvedCodexConfig
            GROK_ALLOW_INSECURE_REMOTE_HTTP = if ($AllowInsecureRemoteHttpProvider) { "1" } else { $null }
        }
    } | ConvertTo-Json -Depth 4
    exit 0
}

function Test-BackendHealth {
    param([string]$Url)
    try {
        $response = Invoke-WebRequest -UseBasicParsing -Uri $Url -TimeoutSec 3
        return [int]$response.StatusCode -eq 200
    }
    catch {
        return $false
    }
}

function Get-BackendModelBinding {
    try {
        return Invoke-RestMethod -Method Get -Uri $modelRuntimeUrl -TimeoutSec 3
    }
    catch {
        return $null
    }
}

function Test-ModelBindingMatch {
    param($Actual, $Expected)
    if ($null -eq $Actual -or $null -eq $Expected) {
        return $false
    }
    $okProperty = $Actual.PSObject.Properties["ok"]
    if ($null -eq $okProperty -or -not [bool]$okProperty.Value) {
        return $false
    }
    foreach ($field in @(
        "backend",
        "providerKey",
        "baseUrl",
        "model",
        "wireApi",
        "reasoningEffort",
        "authEnv",
        "transportSecure",
        "transportAllowed",
        "insecureRemoteHttpOptIn"
    )) {
        $actualProperty = $Actual.PSObject.Properties[$field]
        $expectedProperty = $Expected.PSObject.Properties[$field]
        if (
            $null -eq $actualProperty -or
            $null -eq $expectedProperty -or
            [string]$actualProperty.Value -ne [string]$expectedProperty.Value
        ) {
            return $false
        }
    }
    $actualAuth = $Actual.PSObject.Properties["authAvailable"]
    $expectedAuth = $Expected.PSObject.Properties["authAvailable"]
    if ($null -eq $actualAuth -or $null -eq $expectedAuth) {
        return $false
    }
    return [bool]$actualAuth.Value -eq [bool]$expectedAuth.Value
}

function Stop-OwnedBackendListener {
    $listeners = @(Get-NetTCPConnection -State Listen -LocalPort $gateway.Port -ErrorAction SilentlyContinue)
    $ownerIds = @($listeners | ForEach-Object { [int]$_.OwningProcess } | Sort-Object -Unique)
    if ($ownerIds.Count -ne 1) {
        throw "Cannot safely restart backend on port $($gateway.Port); expected one listener, found $($ownerIds.Count)"
    }
    $owner = Get-CimInstance Win32_Process -Filter "ProcessId=$($ownerIds[0])" -ErrorAction SilentlyContinue
    $ownerExecutable = if ($null -ne $owner -and -not [string]::IsNullOrWhiteSpace($owner.ExecutablePath)) {
        try { [IO.Path]::GetFullPath([string]$owner.ExecutablePath) } catch { $null }
    }
    else {
        $null
    }
    $expectedCommandPattern = '^\s*"?' + [Regex]::Escape($backendPythonExecutable) + '"?\s+-u\s+"?' + [Regex]::Escape($backendScript) + '"?\s*$'
    $isOwnedBackend = (
        $null -ne $owner -and
        $null -ne $ownerExecutable -and
        $ownerExecutable.Equals($backendPythonExecutable, [StringComparison]::OrdinalIgnoreCase) -and
        [Regex]::IsMatch([string]$owner.CommandLine, $expectedCommandPattern, [Text.RegularExpressions.RegexOptions]::IgnoreCase)
    )
    if (-not $isOwnedBackend) {
        throw "Refusing to stop port $($gateway.Port); listener is not this repository's backend_server.py"
    }
    Stop-Process -Id $ownerIds[0] -Force -ErrorAction Stop
    $deadline = (Get-Date).AddSeconds(8)
    do {
        Start-Sleep -Milliseconds 200
        $remaining = @(Get-NetTCPConnection -State Listen -LocalPort $gateway.Port -ErrorAction SilentlyContinue)
    } while ($remaining.Count -gt 0 -and (Get-Date) -lt $deadline)
    if ($remaining.Count -gt 0) {
        throw "Backend listener on port $($gateway.Port) did not stop"
    }
}

function Get-MatchingAppProcesses {
    Get-CimInstance Win32_Process | Where-Object {
        if ([string]::IsNullOrWhiteSpace($_.ExecutablePath)) {
            return $false
        }
        try {
            return [IO.Path]::GetFullPath($_.ExecutablePath).Equals(
                $resolvedApp,
                [StringComparison]::OrdinalIgnoreCase
            )
        }
        catch {
            return $false
        }
    }
}

function Stop-MatchingAppProcesses {
    $matching = @(Get-MatchingAppProcesses)
    if ($matching.Count -eq 0) {
        return
    }
    if (-not $restartExisting) {
        throw "Grok Bot is already running from $resolvedApp; restart is required to apply SAND_HOST_GATEWAY_URL"
    }

    $matchingIds = @($matching | ForEach-Object { [int]$_.ProcessId })
    $matchingSet = [Collections.Generic.HashSet[int]]::new()
    $matchingIds | ForEach-Object { [void]$matchingSet.Add($_) }
    $roots = @($matching | Where-Object { -not $matchingSet.Contains([int]$_.ParentProcessId) })
    foreach ($root in $roots) {
        $process = Get-Process -Id $root.ProcessId -ErrorAction SilentlyContinue
        if ($null -ne $process) {
            [void]$process.CloseMainWindow()
        }
    }

    $deadline = (Get-Date).AddSeconds(8)
    do {
        Start-Sleep -Milliseconds 250
        $remaining = @($matchingIds | Where-Object {
            $null -ne (Get-Process -Id $_ -ErrorAction SilentlyContinue)
        })
    } while ($remaining.Count -gt 0 -and (Get-Date) -lt $deadline)

    if ($remaining.Count -gt 0) {
        Stop-Process -Id $remaining -Force -ErrorAction SilentlyContinue
        Start-Sleep -Seconds 1
    }
}

$backendStarted = $false
$backendRestarted = $false
$backendPid = $null
$backendStdout = $null
$backendStderr = $null
if (-not $SkipBackendHealthCheck -and (Test-BackendHealth -Url $healthUrl)) {
    $actualBinding = Get-BackendModelBinding
    if (-not (Test-ModelBindingMatch -Actual $actualBinding -Expected $modelBinding)) {
        if ($NoRestartBackend) {
            throw "Healthy backend is using a different or unknown model binding; restart is required"
        }
        Stop-OwnedBackendListener
        $backendRestarted = $true
    }
}

if (-not $SkipBackendHealthCheck -and -not (Test-BackendHealth -Url $healthUrl)) {
    if ($NoStartBackend) {
        throw "Local backend is not healthy at $healthUrl"
    }
    if ($gateway.Port -ne 9000) {
        throw "Automatic backend startup only supports loopback port 9000; requested $($gateway.Port)"
    }

    $stamp = Get-Date -Format "yyyyMMdd-HHmmss"
    $backendStdout = Join-Path $repoRoot ".tmp_backend_036_launcher_$stamp.stdout.log"
    $backendStderr = Join-Path $repoRoot ".tmp_backend_036_launcher_$stamp.stderr.log"
    $backend = Invoke-WithModelEnvironment {
        Start-Process `
            -FilePath $python `
            -ArgumentList @("-u", $backendScript) `
            -WorkingDirectory $repoRoot `
            -WindowStyle Hidden `
            -RedirectStandardOutput $backendStdout `
            -RedirectStandardError $backendStderr `
            -PassThru
    }
    $backendStarted = $true
    $backendPid = $backend.Id

    $backendDeadline = (Get-Date).AddSeconds(15)
    do {
        Start-Sleep -Milliseconds 250
        $backendReady = Test-BackendHealth -Url $healthUrl
    } while (-not $backendReady -and (Get-Date) -lt $backendDeadline)
    if (-not $backendReady) {
        throw "Local backend did not become healthy; stdout=$backendStdout stderr=$backendStderr"
    }
    $actualBinding = Get-BackendModelBinding
    if (-not (Test-ModelBindingMatch -Actual $actualBinding -Expected $modelBinding)) {
        throw "Local backend started but did not adopt the requested model binding"
    }
}

Stop-MatchingAppProcesses

$stamp = Get-Date -Format "yyyyMMdd-HHmmss"
$stdout = Join-Path $repoRoot ".tmp_grokbot036_local_gateway_$stamp.stdout.log"
$stderr = Join-Path $repoRoot ".tmp_grokbot036_local_gateway_$stamp.stderr.log"
$hadGatewayEnv = Test-Path Env:SAND_HOST_GATEWAY_URL
$previousGatewayEnv = if ($hadGatewayEnv) { $env:SAND_HOST_GATEWAY_URL } else { $null }
try {
    $env:SAND_HOST_GATEWAY_URL = $gatewayBase
    $app = Start-Process `
        -FilePath $resolvedApp `
        -WorkingDirectory (Split-Path -Parent $resolvedApp) `
        -RedirectStandardOutput $stdout `
        -RedirectStandardError $stderr `
        -PassThru
}
finally {
    if ($hadGatewayEnv) {
        $env:SAND_HOST_GATEWAY_URL = $previousGatewayEnv
    }
    else {
        Remove-Item Env:SAND_HOST_GATEWAY_URL -ErrorAction SilentlyContinue
    }
}

$deadline = (Get-Date).AddSeconds($StartupTimeoutSeconds)
$coordinatorPids = @()
do {
    if ($null -eq (Get-Process -Id $app.Id -ErrorAction SilentlyContinue)) {
        throw "Grok Bot exited before the local coordinator connected; stdout=$stdout stderr=$stderr"
    }

    $matching = @(Get-MatchingAppProcesses)
    $matchingIds = [Collections.Generic.HashSet[int]]::new()
    $matching | ForEach-Object { [void]$matchingIds.Add([int]$_.ProcessId) }
    $connections = @(Get-NetTCPConnection -State Established -RemotePort $gateway.Port -ErrorAction SilentlyContinue)
    $coordinatorPids = @(
        $connections |
            Where-Object { $matchingIds.Contains([int]$_.OwningProcess) -and $_.OwningProcess -ne $app.Id } |
            ForEach-Object {
                $owner = Get-CimInstance Win32_Process -Filter "ProcessId=$($_.OwningProcess)" -ErrorAction SilentlyContinue
                if ($null -ne $owner -and ([string]$owner.CommandLine) -notmatch "local-exec-daemon") {
                    [int]$owner.ProcessId
                }
            } |
            Sort-Object -Unique
    )
    if ($coordinatorPids.Count -gt 0) {
        break
    }
    Start-Sleep -Milliseconds 250
} while ((Get-Date) -lt $deadline)

if ($coordinatorPids.Count -eq 0) {
    throw "Grok Bot started, but its coordinator did not connect to $gatewayBase within $StartupTimeoutSeconds seconds; stdout=$stdout stderr=$stderr"
}

$providerReady = [bool]$modelBinding.authAvailable
$providerProbeType = "auth-presence"
$ollamaReady = $null
if ($modelBinding.backend -eq "ollama") {
    $providerProbeType = "ollama-tags"
    try {
        $ollama = Invoke-WebRequest -UseBasicParsing -Uri "$($modelBinding.baseUrl)/api/tags" -TimeoutSec 3
        $ollamaReady = [int]$ollama.StatusCode -eq 200
        $providerReady = $ollamaReady
    }
    catch {
        $ollamaReady = $false
        $providerReady = $false
    }
}

[ordered]@{
    status = "ready"
    appPid = $app.Id
    coordinatorPids = $coordinatorPids
    gatewayUrl = $gatewayBase
    backendStarted = $backendStarted
    backendRestarted = $backendRestarted
    backendPid = $backendPid
    modelBinding = $modelBinding
    providerReady = $providerReady
    providerProbeType = $providerProbeType
    providerRequestSent = $false
    ollamaReady = $ollamaReady
    stdout = $stdout
    stderr = $stderr
    backendStdout = $backendStdout
    backendStderr = $backendStderr
} | ConvertTo-Json -Depth 4
