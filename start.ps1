<#
.SYNOPSIS
  Khoi dong toan bo Lakehouse va chay pipeline 69 job trong 1 lenh.

  Buoc 1: bat Docker Desktop (neu chua chay), kiem tra VPN toi Oracle
  Buoc 2: docker compose up (Postgres, MinIO, Spark, dbt, Airflow, Trino, CloudBeaver)
  Buoc 3: sinh + phuc vu dbt docs (lineage) tai http://localhost:8082
  Buoc 4: trigger DAG dag_lakehouse_daily tren Airflow va theo doi den khi xong

.EXAMPLE
  .\start.ps1                       # bat he thong + chay pipeline (ngay = Variable etl_date, mac dinh 2025-10-07)
  .\start.ps1 -EtlDate 2025-10-07   # chay pipeline cho 1 ngay cu the
  .\start.ps1 -NoRun                # chi bat he thong + dbt docs, khong chay pipeline
  .\start.ps1 -NoWait               # trigger pipeline roi thoat, khong cho chay xong
  .\start.ps1 -Build                # rebuild image (sau khi sua thu muc docker/)
  .\start.ps1 -Stop                 # tat toan bo (giu nguyen du lieu)

  Neu PowerShell chan script: powershell -ExecutionPolicy Bypass -File .\start.ps1
#>
param(
    [string]$EtlDate,
    [switch]$NoRun,
    [switch]$NoWait,
    [switch]$Build,
    [switch]$Stop
)

$ErrorActionPreference = 'Continue'
Set-Location $PSScriptRoot
$Profiles = @('--profile', 'airflow', '--profile', 'ui')

function Step([string]$m) { Write-Host "`n==> $m" -ForegroundColor Cyan }
function Fail([string]$m) { Write-Host "LOI: $m" -ForegroundColor Red; exit 1 }

# Doc .env (POSTGRES_USER, ORACLE_HOST, SOURCE_MODE ...)
$EnvFile = @{}
if (-not (Test-Path .env)) { Fail ".env khong ton tai - tao tu .env.example" }
Get-Content .env | Where-Object { $_ -match '^\s*([A-Z_]+)\s*=\s*(.*)$' } | ForEach-Object {
    $EnvFile[$matches[1]] = $matches[2].Trim()
}
$PgUser = if ($EnvFile.POSTGRES_USER) { $EnvFile.POSTGRES_USER } else { 'airflow' }

if ($Stop) {
    Step "Tat toan bo container (du lieu giu nguyen)"
    docker compose @Profiles stop
    exit 0
}

# ---------------------------------------------------------------- Buoc 1 --
Step "Kiem tra Docker"
docker info *> $null
if ($LASTEXITCODE -ne 0) {
    $exe = @("$env:ProgramFiles\Docker\Docker\Docker Desktop.exe",
             "$env:LOCALAPPDATA\Programs\DockerDesktop\Docker Desktop.exe") |
           Where-Object { Test-Path $_ } | Select-Object -First 1
    if (-not $exe) { Fail "Khong tim thay Docker Desktop" }
    Write-Host "Dang mo Docker Desktop..."
    Start-Process $exe
    for ($i = 0; $i -lt 60; $i++) {
        Start-Sleep -Seconds 5
        docker info *> $null
        if ($LASTEXITCODE -eq 0) { break }
    }
    if ($LASTEXITCODE -ne 0) { Fail "Docker chua san sang sau 5 phut" }
}
Write-Host "Docker OK"

if ($EnvFile.SOURCE_MODE -ne 'mock' -and $EnvFile.ORACLE_HOST) {
    Step "Kiem tra VPN toi Oracle $($EnvFile.ORACLE_HOST):$($EnvFile.ORACLE_PORT)"
    $port = if ($EnvFile.ORACLE_PORT) { [int]$EnvFile.ORACLE_PORT } else { 1521 }
    $ok = Test-NetConnection $EnvFile.ORACLE_HOST -Port $port -InformationLevel Quiet -WarningAction SilentlyContinue
    if ($ok) { Write-Host "Oracle OK" }
    elseif (-not $NoRun) { Fail "Khong ket noi duoc Oracle - bat VPN roi chay lai (hoac dung -NoRun de chi bat giao dien)" }
    else { Write-Host "Oracle chua ket noi (VPN tat) - van tiep tuc vi -NoRun" -ForegroundColor Yellow }
}

# ---------------------------------------------------------------- Buoc 2 --
Step "Khoi dong container"
$upArgs = @('compose') + $Profiles + @('up', '-d')
if ($Build) { $upArgs += '--build' }
& docker @upArgs
if ($LASTEXITCODE -ne 0) { Fail "docker compose up that bai" }

Write-Host "Cho Airflow san sang..."
$ready = $false
for ($i = 0; $i -lt 60; $i++) {
    try {
        $h = Invoke-WebRequest -UseBasicParsing http://localhost:8081/health -TimeoutSec 5
        if ($h.Content -match '"scheduler":\s*\{[^}]*"status":\s*"healthy"') { $ready = $true; break }
    } catch { }
    Start-Sleep -Seconds 5
}
if (-not $ready) { Fail "Airflow chua san sang sau 5 phut - xem: docker compose logs airflow" }
Write-Host "Airflow OK"

# ---------------------------------------------------------------- Buoc 3 --
Step "Sinh dbt docs (lineage) - http://localhost:8082"
$docsDate = if ($EtlDate) { $EtlDate } else { '2025-10-07' }
docker compose exec -T dbt bash /usr/app/scripts/dbt_docs.sh $docsDate
if ($LASTEXITCODE -ne 0) { Write-Host "dbt docs loi (bo qua, khong anh huong pipeline)" -ForegroundColor Yellow }

$Urls = @"

  Airflow      http://localhost:8081   (user $PgUser / POSTGRES_PASSWORD trong .env)
  CloudBeaver  http://localhost:8978   (SQL editor, ket noi Trino co san)
  Trino UI     http://localhost:8080   (DBeaver: Trino localhost:8080, user admin)
  dbt docs     http://localhost:8082   (lineage)
  MinIO        http://localhost:9001   (MINIO_ROOT_USER / MINIO_ROOT_PASSWORD)
  Spark UI     http://localhost:4040
"@

if ($NoRun) {
    Step "He thong da san sang (khong chay pipeline vi -NoRun)"
    Write-Host $Urls
    exit 0
}

# ---------------------------------------------------------------- Buoc 4 --
Step "Trigger DAG dag_lakehouse_daily"
docker compose exec -T airflow airflow dags unpause dag_lakehouse_daily *> $null
$runId = "manual__start_" + (Get-Date -Format 'yyyyMMdd_HHmmss')
$trigArgs = @('compose', 'exec', '-T', 'airflow', 'airflow', 'dags', 'trigger', 'dag_lakehouse_daily', '-r', $runId)
if ($EtlDate) {
    # PowerShell 5.1 bo dau " khi truyen cho exe -> can escape \" ; PowerShell 7.3+ truyen nguyen van
    $conf = if ($PSVersionTable.PSVersion.Major -ge 7) { '{"etl_date": "' + $EtlDate + '"}' }
            else { '{\"etl_date\": \"' + $EtlDate + '\"}' }
    $trigArgs += @('-c', $conf)
}
& docker @trigArgs *> $null
if ($LASTEXITCODE -ne 0) { Fail "Trigger DAG that bai" }
$dateMsg = if ($EtlDate) { $EtlDate } else { 'Variable etl_date' }
Write-Host "Da trigger run $runId (ngay nghiep vu: $dateMsg)"
Start-Process "http://localhost:8081/dags/dag_lakehouse_daily/grid?tab=graph"

if ($NoWait) { Write-Host $Urls; exit 0 }

Step "Theo doi pipeline (Ctrl+C de thoat, pipeline van chay tiep)"
$sqlState = "select state from dag_run where run_id='$runId'"
$sqlTasks = "select string_agg(s||'='||n, '  ' order by s) from (select coalesce(state,'none') s, count(*) n from task_instance where run_id='$runId' group by 1) x"
$start = Get-Date
do {
    Start-Sleep -Seconds 20
    $state = (docker compose exec -T postgres psql -U $PgUser -d airflow -tAc $sqlState) -join ''
    $tasks = (docker compose exec -T postgres psql -U $PgUser -d airflow -tAc $sqlTasks) -join '  '
    $mins = [math]::Round(((Get-Date) - $start).TotalMinutes, 1)
    Write-Host ("[{0,5} phut] {1,-8} {2}" -f $mins, $state.Trim(), $tasks)
} while ($state.Trim() -in @('queued', 'running', ''))

$failed = docker compose exec -T postgres psql -U $PgUser -d airflow -tAc `
    "select task_id from task_instance where run_id='$runId' and state in ('failed','upstream_failed')"
if ($state.Trim() -eq 'success') {
    Step "PIPELINE THANH CONG"
} else {
    Step "PIPELINE KET THUC VOI TRANG THAI: $state"
    Write-Host "Task loi:" -ForegroundColor Red
    $failed | Where-Object { $_.Trim() } | ForEach-Object { Write-Host "  $_" -ForegroundColor Red }
    Write-Host "Xem log tren Airflow (bam vao task -> Logs)."
}
Write-Host "So dong tung tang: Airflow -> task report_layer_row_counts -> Logs"
Write-Host $Urls
