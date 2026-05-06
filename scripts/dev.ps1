param(
    [switch]$Web,
    [switch]$KillPorts
)

$ErrorActionPreference = "Stop"

function Write-Step([string]$Message) {
    Write-Host ("[dev] " + $Message)
}

function Get-ListeningProcessIds([int]$Port) {
    try {
        return (Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction Stop |
            Select-Object -ExpandProperty OwningProcess -Unique)
    }
    catch {
        return @()
    }
}

Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned | Out-Null

$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot ".."))
Set-Location $RepoRoot

$PythonExe = Join-Path $RepoRoot ".venv\Scripts\python.exe"

if (-not (Test-Path $PythonExe)) {
    Write-Step "Criando virtualenv em .venv..."
    if (Get-Command py -ErrorAction SilentlyContinue) {
        py -3.11 -m venv .venv
    }
    else {
        python -m venv .venv
    }
}

$RequirementsPath = Join-Path $RepoRoot "requirements.txt"
$RequirementsHash = (Get-FileHash $RequirementsPath -Algorithm SHA256).Hash
$RequirementsStamp = Join-Path $RepoRoot ".venv\.requirements.sha256"

$NeedPipInstall = $true
if (Test-Path $RequirementsStamp) {
    $PreviousHash = (Get-Content $RequirementsStamp -Raw).Trim()
    if ($PreviousHash -eq $RequirementsHash) {
        $NeedPipInstall = $false
    }
}

# Se alguém limpou pacotes dentro do venv, detecta via import e reinstala.
$ImportCheck = "import fastapi, uvicorn, sqlalchemy, flet, dotenv, pandas, requests"
& $PythonExe -c $ImportCheck 2>$null
if ($LASTEXITCODE -ne 0) {
    $NeedPipInstall = $true
}

if ($NeedPipInstall) {
    Write-Step "Instalando dependencias Python (requirements.txt)..."
    & $PythonExe -m pip install -r $RequirementsPath
    Set-Content -Path $RequirementsStamp -Value $RequirementsHash -NoNewline
}
else {
    Write-Step "Dependencias Python ok (sem reinstalar)."
}

$PortsToCheck = @(8000, 8550)
$Busy = @()
foreach ($Port in $PortsToCheck) {
    $ListeningProcessIds = Get-ListeningProcessIds -Port $Port
    foreach ($ProcessId in $ListeningProcessIds) {
        $Busy += [PSCustomObject]@{ Port = $Port; ProcessId = $ProcessId }
    }
}

if ($Busy.Count -gt 0) {
    $Summary = ($Busy | Sort-Object Port, ProcessId | ForEach-Object { "$($_.Port) (ProcessId $($_.ProcessId))" }) -join ", "
    if ($KillPorts) {
        Write-Step "Portas ocupadas detectadas: $Summary. Encerrando processos..."
        $ProcessIdsToStop = ($Busy | Select-Object -ExpandProperty ProcessId -Unique)
        foreach ($ProcessId in $ProcessIdsToStop) {
            try { Stop-Process -Id $ProcessId -Force -ErrorAction Stop } catch { }
        }
    }
    else {
        Write-Step "Portas ocupadas detectadas: $Summary."
        Write-Step "Feche a outra instancia (Ctrl+C) ou rode novamente com -KillPorts."
        exit 2
    }
}

if ($Web) {
    $NodeModules = Join-Path $RepoRoot "node_modules"
    $PackageLock = Join-Path $RepoRoot "package-lock.json"

    if (-not (Test-Path $NodeModules)) {
        Write-Step "Instalando dependencias Node (npm ci)..."
        if (Test-Path $PackageLock) { npm ci } else { npm install }
    }

    Write-Step "Subindo API + Web (Vite)..."
    npm run dev:full
}
else {
    Write-Step "Subindo API + Flet..."
    & $PythonExe app.py
}
