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

function Test-PortBindable([int]$Port) {
    try {
        $Listener = New-Object System.Net.Sockets.TcpListener([System.Net.IPAddress]::Loopback, $Port)
        $Listener.Start()
        $Listener.Stop()
        return $true
    }
    catch {
        try { if ($Listener) { $Listener.Stop() } } catch { }
        return $false
    }
}

function Select-FreePort([int]$PreferredPort, [int]$MaxAttempts = 20) {
    if (Test-PortBindable -Port $PreferredPort) { return $PreferredPort }
    for ($i = 1; $i -le $MaxAttempts; $i++) {
        $Candidate = $PreferredPort + $i
        if ($Candidate -ge 1 -and $Candidate -lt 65536) {
            if (Test-PortBindable -Port $Candidate) { return $Candidate }
        }
    }
    throw "Nao foi possivel encontrar uma porta livre a partir de $PreferredPort."
}

Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned | Out-Null

$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot ".."))
Set-Location $RepoRoot

# Porta da API: por padrão 8000, mas permite override via env (PORT)
$ApiPort = 8000
try {
    if ($env:PORT) {
        $Parsed = [int]$env:PORT
        if ($Parsed -gt 0 -and $Parsed -lt 65536) { $ApiPort = $Parsed }
    }
}
catch { }

function Invoke-CheckedExternal([string]$Label, [scriptblock]$Command) {
    $PreviousEap = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        & $Command
    }
    finally {
        $ErrorActionPreference = $PreviousEap
    }

    if ($LASTEXITCODE -ne 0) {
        throw "${Label} falhou (exit code $LASTEXITCODE)."
    }
}

$PythonExe = Join-Path $RepoRoot ".venv\Scripts\python.exe"

if (Test-Path $PythonExe) {
    $VenvOk = $true
    try {
        $PreviousEap = $ErrorActionPreference
        $ErrorActionPreference = "SilentlyContinue"
        & $PythonExe --version 1>$null 2>$null
        $ErrorActionPreference = $PreviousEap
        if ($LASTEXITCODE -ne 0) { $VenvOk = $false }
    }
    catch {
        try { $ErrorActionPreference = $PreviousEap } catch { }
        $VenvOk = $false
    }

    if (-not $VenvOk) {
        Write-Step "Virtualenv existente em .venv parece quebrada; recriando..."
        try { Remove-Item -Path (Join-Path $RepoRoot ".venv") -Recurse -Force -ErrorAction Stop } catch { }
    }
}

if (-not (Test-Path $PythonExe)) {
    Write-Step "Criando virtualenv em .venv..."

    $VenvCreated = $false

    if (Get-Command python -ErrorAction SilentlyContinue) {
        Invoke-CheckedExternal "python -m venv .venv" { python -m venv .venv }
        $VenvCreated = $true
    }
    elseif (Get-Command py -ErrorAction SilentlyContinue) {
        try {
            Invoke-CheckedExternal "py -3 -m venv .venv" { py -3 -m venv .venv }
            $VenvCreated = $true
        }
        catch {
            Invoke-CheckedExternal "py -m venv .venv" { py -m venv .venv }
            $VenvCreated = $true
        }
    }

    if (-not $VenvCreated) {
        throw "Nao foi possivel criar a virtualenv em .venv (Python nao encontrado ou falha ao executar venv)."
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
$ImportExitCode = 0
try {
    $PreviousEap = $ErrorActionPreference
    $ErrorActionPreference = "SilentlyContinue"
    & $PythonExe -c $ImportCheck 1>$null 2>$null
    $ImportExitCode = $LASTEXITCODE
    $ErrorActionPreference = $PreviousEap
}
catch {
    try { $ErrorActionPreference = $PreviousEap } catch { }
    $ImportExitCode = 1
}

if ($ImportExitCode -ne 0) {
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

$PortsToCheck = @($ApiPort, 8550)
if ($Web) {
    $PortsToCheck += 5173
}
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

# Mesmo com -KillPorts, pode existir listener “fantasma” que o Get-NetTCPConnection nao lista.
# Entao validamos bind de verdade e, se preciso, escolhemos outra porta e sincronizamos o Vite.
if (-not (Test-PortBindable -Port $ApiPort)) {
    $OldApiPort = $ApiPort
    $ApiPort = Select-FreePort -PreferredPort $ApiPort -MaxAttempts 50
    Write-Step "Porta $OldApiPort indisponivel (bind falhou). Usando $ApiPort."
}

# Mantem API e proxy do Vite alinhados sem exigir .env.
$env:PORT = "$ApiPort"
$env:VITE_API_PORT = "$ApiPort"

if ($Web) {
    $NodeModules = Join-Path $RepoRoot "node_modules"
    $PackageLock = Join-Path $RepoRoot "package-lock.json"

    if (-not (Test-Path $NodeModules)) {
        Write-Step "Instalando dependencias Node (npm ci)..."
        if (Test-Path $PackageLock) { npm ci } else { npm install }
    }

    Write-Step "Subindo API + Web (Vite)..."

    $ViteBin = Join-Path $RepoRoot "node_modules\vite\bin\vite.js"
    if (-not (Test-Path $ViteBin)) {
        throw "vite nao encontrado em node_modules (esperado: $ViteBin). Rode npm install/ci."
    }

    Write-Step "Iniciando Web (Vite) em http://0.0.0.0:5173 (acesse via http://<IP-da-maquina>:5173) ..."
    $WebProc = Start-Process -FilePath "node" -ArgumentList @($ViteBin, "--host", "0.0.0.0", "--port", "5173") -WorkingDirectory $RepoRoot -PassThru

    try {
        Write-Step "Iniciando API (Uvicorn) em http://0.0.0.0:$ApiPort (LAN: http://<IP-da-maquina>:$ApiPort) ..."
        & $PythonExe -m uvicorn backend.main:app --reload --reload-dir backend --host 0.0.0.0 --port $ApiPort
    }
    finally {
        if ($WebProc -and -not $WebProc.HasExited) {
            try { Stop-Process -Id $WebProc.Id -Force -ErrorAction Stop } catch { }
        }
    }
}
else {
    Write-Step "Subindo API + Flet..."
    & $PythonExe app.py
}
