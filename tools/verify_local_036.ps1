[CmdletBinding()]
param(
    [switch]$DryRun,
    [string]$CodexConfigPath = (Join-Path $HOME ".codex\config.toml"),
    [switch]$AllowInsecureRemoteHttpProvider
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
if (Test-Path Variable:PSNativeCommandUseErrorActionPreference) {
    $PSNativeCommandUseErrorActionPreference = $false
}

$repoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
$python = Join-Path $repoRoot ".venv\Scripts\python.exe"
$routingCheck = Join-Path $repoRoot "tools\patch_local_routing_036.py"
$rendererCheck = Join-Path $repoRoot "tools\patch_renderer_036.py"
$modelRuntime = Join-Path $repoRoot "model_runtime.py"
$launcher = Join-Path $repoRoot "tools\start_grok_bot_036_local.ps1"
$secretScan = Join-Path $repoRoot "scripts\secret_scan.py"
$pwsh = Join-Path $PSHOME "pwsh.exe"

$requiredFiles = @($python, $routingCheck, $rendererCheck, $modelRuntime, $launcher, $secretScan, $pwsh, $CodexConfigPath)
foreach ($path in $requiredFiles) {
    if (-not (Test-Path -LiteralPath $path -PathType Leaf)) {
        throw "Required local verification input is missing: $path"
    }
}
$resolvedCodexConfig = [IO.Path]::GetFullPath((Resolve-Path -LiteralPath $CodexConfigPath).Path)

$testTargets = @(
    Get-ChildItem -LiteralPath (Join-Path $repoRoot "tests") -Filter "test_*.py" -File |
        Sort-Object Name |
        ForEach-Object { "tests\$($_.Name)" }
)
$compileTargets = @(
    "backend_server.py"
    "model_runtime.py"
    "scripts\secret_scan.py"
    "tools\patch_local_routing_036.py"
    "tools\patch_renderer_036.py"
) + $testTargets

$releaseTargets = @(
    ".github/workflows/ci.yml"
    ".gitignore"
    "README.md"
    "backend_server.py"
    "CHANGELOG.md"
    "config.example.json"
    "docs/current-vs-original-2026-09-01.md"
    "docs/handlers.md"
    "model_runtime.py"
    "scripts/secret_scan.py"
    "tests/test_connect_stream.py"
    "tests/test_local_launcher_036.py"
    "tests/test_local_verify_036.py"
    "tests/test_model_runtime.py"
    "tests/test_release_policy.py"
    "tests/test_renderer_patch_036.py"
    "tests/test_secret_scan.py"
    "TODOS.md"
    "tools/patch_local_routing_036.py"
    "tools/patch_renderer_036.py"
    "tools/start_grok_bot_036_local.ps1"
    "tools/verify_local_036.ps1"
    "VERSION"
)

$launcherArguments = @(
    "-NoLogo",
    "-NoProfile",
    "-File",
    $launcher,
    "-CodexConfigPath",
    $resolvedCodexConfig,
    "-DryRun",
    "-SkipBackendHealthCheck",
    "-NoStartBackend"
)
if ($AllowInsecureRemoteHttpProvider) {
    $launcherArguments += "-AllowInsecureRemoteHttpProvider"
}

$steps = @(
    [ordered]@{
        name = "routing_check"
        enabled = $true
        executable = $python
        arguments = @($routingCheck, "--check")
    }
    [ordered]@{
        name = "renderer_check"
        enabled = $true
        executable = $python
        arguments = @($rendererCheck, "--check")
    }
    [ordered]@{
        name = "model_binding_check"
        enabled = $true
        executable = $python
        arguments = @($modelRuntime, "--require-auth", "--codex-config", $resolvedCodexConfig)
    }
    [ordered]@{
        name = "launcher_dry_run"
        enabled = $true
        executable = $pwsh
        arguments = $launcherArguments
    }
    [ordered]@{
        name = "python_compile"
        enabled = $true
        executable = $python
        arguments = @("-m", "py_compile") + $compileTargets
    }
    [ordered]@{
        name = "unit_tests"
        enabled = $true
        executable = $python
        arguments = @("-m", "unittest", "discover", "-s", "tests", "-p", "test_*.py", "-v")
    }
    [ordered]@{
        name = "secret_scan_tracked"
        enabled = $true
        executable = $python
        arguments = @($secretScan)
    }
    [ordered]@{
        name = "secret_scan_staged"
        enabled = $true
        executable = $python
        arguments = @($secretScan, "--staged")
    }
    [ordered]@{
        name = "secret_scan_write_set"
        enabled = $true
        executable = $python
        arguments = @($secretScan, "--write-set") + $releaseTargets
    }
    [ordered]@{
        name = "git_diff_check"
        enabled = $true
        executable = "git"
        arguments = @("diff", "--check")
    }
)

if ($DryRun) {
    [ordered]@{
        version = "0.36.0"
        mode = "plan-only"
        localOnly = $false
        networkPolicy = "loopback-gateway-with-explicit-model-provider"
        startsGui = $false
        startsProvider = $false
        sendsProviderRequest = $false
        codexConfigPath = $resolvedCodexConfig
        allowInsecureRemoteHttpProvider = [bool]$AllowInsecureRemoteHttpProvider
        steps = $steps
    } | ConvertTo-Json -Depth 6
    exit 0
}

function Invoke-NativeStep {
    param(
        [Parameter(Mandatory)]
        [string]$Name,
        [Parameter(Mandatory)]
        [string]$Executable,
        [Parameter(Mandatory)]
        [object[]]$Arguments
    )

    Write-Host "==> $Name"
    $nativeArguments = [string[]]$Arguments
    & $Executable @nativeArguments
    $exitCode = $LASTEXITCODE
    if ($exitCode -ne 0) {
        throw "$Name failed with exit code $exitCode"
    }
}

Push-Location $repoRoot
try {
    Write-Host "==> routing_check"
    $routingExecutable = [string]$steps[0].executable
    $routingArguments = [string[]]$steps[0].arguments
    $routingOutput = @(& $routingExecutable @routingArguments 2>&1)
    $routingExitCode = $LASTEXITCODE
    $routingOutput | ForEach-Object { Write-Host ([string]$_) }
    if ($routingExitCode -ne 0) {
        throw "routing_check failed with exit code $routingExitCode"
    }
    $routingText = $routingOutput -join "`n"
    if ($routingText -match "(?m)^needs-patch:") {
        throw "routing_check found a Grok Bot 0.36 bundle that still needs local-only defaults"
    }
    if ([regex]::Matches($routingText, "(?m)^already-patched:").Count -lt 2) {
        throw "routing_check did not verify both Grok Bot 0.36 main bundles"
    }

    Write-Host "==> renderer_check"
    $rendererExecutable = [string]$steps[1].executable
    $rendererArguments = [string[]]$steps[1].arguments
    $rendererOutput = @(& $rendererExecutable @rendererArguments 2>&1)
    $rendererExitCode = $LASTEXITCODE
    $rendererOutput | ForEach-Object { Write-Host ([string]$_) }
    if ($rendererExitCode -ne 0) {
        throw "renderer_check failed with exit code $rendererExitCode"
    }
    $rendererText = $rendererOutput -join "`n"
    if ($rendererText -match "(?m)^needs-patch:") {
        throw "renderer_check found a Grok Bot 0.36 renderer that still needs local-only patches"
    }
    if ([regex]::Matches($rendererText, "(?m)^already-patched:").Count -lt 2) {
        throw "renderer_check did not verify both Grok Bot 0.36 renderer bundles"
    }

    Write-Host "==> model_binding_check"
    $launcherExecutable = [string]$steps[2].executable
    $launcherArguments = [string[]]$steps[2].arguments
    $hadInsecureHttpOptIn = Test-Path Env:GROK_ALLOW_INSECURE_REMOTE_HTTP
    $previousInsecureHttpOptIn = if ($hadInsecureHttpOptIn) { $env:GROK_ALLOW_INSECURE_REMOTE_HTTP } else { $null }
    try {
        if ($AllowInsecureRemoteHttpProvider) {
            $env:GROK_ALLOW_INSECURE_REMOTE_HTTP = "1"
        }
        else {
            Remove-Item Env:GROK_ALLOW_INSECURE_REMOTE_HTTP -ErrorAction SilentlyContinue
        }
        $modelOutput = @(& $launcherExecutable @launcherArguments 2>&1)
        $modelExitCode = $LASTEXITCODE
    }
    finally {
        if ($hadInsecureHttpOptIn) {
            $env:GROK_ALLOW_INSECURE_REMOTE_HTTP = $previousInsecureHttpOptIn
        }
        else {
            Remove-Item Env:GROK_ALLOW_INSECURE_REMOTE_HTTP -ErrorAction SilentlyContinue
        }
    }
    if ($modelExitCode -ne 0) {
        $modelOutput | ForEach-Object { Write-Host ([string]$_) }
        throw "model_binding_check failed with exit code $modelExitCode"
    }
    $modelText = $modelOutput -join "`n"
    try {
        $modelPlan = $modelText | ConvertFrom-Json
    }
    catch {
        throw "model_binding_check did not return valid JSON: $modelText"
    }
    if (
        -not [bool]$modelPlan.ok -or
        $modelPlan.backend -ne "responses" -or
        $modelPlan.wireApi -ne "responses" -or
        [string]::IsNullOrWhiteSpace([string]$modelPlan.model) -or
        [string]::IsNullOrWhiteSpace([string]$modelPlan.authEnv) -or
        -not [bool]$modelPlan.authAvailable -or
        -not [bool]$modelPlan.transportAllowed
    ) {
        throw "model_binding_check did not resolve an authenticated Responses binding"
    }
    Write-Host $modelText

    Write-Host "==> launcher_dry_run"
    $launcherExecutable = [string]$steps[3].executable
    $launcherArguments = [string[]]$steps[3].arguments
    $launcherOutput = @(& $launcherExecutable @launcherArguments 2>&1)
    $launcherExitCode = $LASTEXITCODE
    if ($launcherExitCode -ne 0) {
        $launcherOutput | ForEach-Object { Write-Host ([string]$_) }
        throw "launcher_dry_run failed with exit code $launcherExitCode"
    }
    $launcherText = $launcherOutput -join "`n"
    try {
        $launcherPlan = $launcherText | ConvertFrom-Json
    }
    catch {
        throw "launcher_dry_run did not return valid JSON: $launcherText"
    }
    if (
        $launcherPlan.gatewayUrl -ne "http://127.0.0.1:9000" -or
        $launcherPlan.healthUrl -ne "http://127.0.0.1:9000/health" -or
        $launcherPlan.environment.SAND_HOST_GATEWAY_URL -ne "http://127.0.0.1:9000" -or
        $launcherPlan.environment.GROK_MODEL_BACKEND -ne "codex" -or
        $launcherPlan.environment.GROK_CODEX_CONFIG_PATH -ne $resolvedCodexConfig -or
        $launcherPlan.modelBinding.backend -ne "responses" -or
        $launcherPlan.modelBinding.wireApi -ne "responses" -or
        -not [bool]$launcherPlan.modelBinding.authAvailable -or
        -not [bool]$launcherPlan.modelBinding.transportAllowed -or
        [bool]$launcherPlan.allowInsecureRemoteHttpProvider -ne [bool]$AllowInsecureRemoteHttpProvider -or
        $launcherPlan.startBackendIfMissing -ne $false
    ) {
        throw "launcher_dry_run is not bound to the expected local gateway and Codex Responses contract"
    }
    Write-Host $launcherText

    foreach ($step in $steps[4..($steps.Count - 1)]) {
        if (-not [bool]$step.enabled) {
            Write-Host "==> $($step.name) (skipped: empty write-set)"
            continue
        }
        Invoke-NativeStep `
            -Name ([string]$step.name) `
            -Executable ([string]$step.executable) `
            -Arguments ([object[]]$step.arguments)
    }
}
finally {
    Pop-Location
}

Write-Host "[local-verify-036] PASS"
