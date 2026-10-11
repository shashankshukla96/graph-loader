# Windows PowerShell equivalent of start_dev.sh.
# Requires Docker Desktop for Windows with Docker Compose v2.

$projectRoot = Split-Path -Parent $PSScriptRoot
Push-Location $projectRoot

function Wait-ForHealthyContainer {
    param(
        [Parameter(Mandatory = $true)] [string] $ContainerName,
        [Parameter(Mandatory = $true)] [string] $ServiceName
    )

    $deadline = (Get-Date).AddSeconds(120)
    Write-Host "Waiting for $ServiceName to be healthy..."
    while ((Get-Date) -lt $deadline) {
        $status = docker inspect $ContainerName --format='{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' 2>$null
        if ($status -eq "healthy") {
            Write-Host "$ServiceName is ready."
            return $true
        }
        if ($status -eq "unhealthy") {
            Write-Error "$ServiceName became unhealthy. Recent logs:"
            docker compose logs --tail 100 $ServiceName
            return $false
        }
        Start-Sleep -Seconds 3
    }

    Write-Error "$ServiceName did not become healthy within 120 seconds. Recent logs:"
    docker compose logs --tail 100 $ServiceName
    return $false
}

try {
    if (-not (Test-Path ".env")) {
        Write-Error "Warning: .env not found. Copy .env.example to .env and set NEO4J_PASSWORD."
        exit 1
    }

    Write-Host "Starting Graph Loader dev stack..."
    docker compose up -d
    if ($LASTEXITCODE -ne 0) {
        Write-Error "docker compose up failed with exit code $LASTEXITCODE."
        exit $LASTEXITCODE
    }

    if (-not (Wait-ForHealthyContainer -ContainerName "graph-loader-neo4j" -ServiceName "neo4j")) {
        exit 1
    }
    if (-not (Wait-ForHealthyContainer -ContainerName "graph-loader-kafka" -ServiceName "kafka")) {
        exit 1
    }

    Write-Host "`nDev stack is up:"
    Write-Host "  Neo4j Browser : http://localhost:7474"
    Write-Host "  Neo4j Bolt    : bolt://localhost:7687"
    Write-Host "  Kafka         : localhost:9092"
}
finally {
    Pop-Location
}
