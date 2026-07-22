param(
    [string]$Etcd = "127.0.0.1:2379",
    [string]$DbPath = ".\shortlink.db"
)

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $ScriptDir

Write-Host "=========================================="
Write-Host " ReachCache Shortlink Demo"
Write-Host " etcd=$Etcd  db=$DbPath"
Write-Host "=========================================="

$global:Processes = @()

function Cleanup {
    Write-Host ""
    Write-Host "shutting down..."
    foreach ($p in $global:Processes) {
        if ( -not $p.HasExited ) { $p.Kill() }
    }
    Write-Host "done"
}
Register-EngineEvent -SourceIdentifier PowerShell.Exiting -SupportEvent -Action { Cleanup } | Out-Null

function Start-Node($name, $grpc, $http, $advertise) {
    $p = Start-Process -NoNewWindow -PassThru powershell -ArgumentList @(
        "-NoProfile", "-Command", "go run . -name $name -addr $grpc -http $http -advertise $advertise -etcd '$Etcd' -db '$DbPath'"
    )
    $global:Processes += $p
    return $p
}

$pA = Start-Node "node-a" ":50051" ":8081" "127.0.0.1:50051"
Start-Sleep -Seconds 1

$pB = Start-Node "node-b" ":50052" ":8082" "127.0.0.1:50052"
Start-Sleep -Seconds 1

$pC = Start-Node "node-c" ":50053" ":8083" "127.0.0.1:50053"

Write-Host ""
Write-Host "All nodes started. Demo commands:"
Write-Host ""
Write-Host "  # Create a short link via Node A"
Write-Host "  Invoke-RestMethod -Method Post -Uri 'http://localhost:8081/shorten?url=https://github.com/vernmorn/reachcache'"
Write-Host ""
Write-Host "  # Access it via Node B (consistency hash routes to owner)"
Write-Host "  Invoke-WebRequest -Uri http://localhost:8082/<code>"
Write-Host ""
Write-Host "  # Check cache stats on Node C"
Write-Host "  Invoke-RestMethod -Uri http://localhost:8083/stats"
Write-Host ""

Write-Host "Press Ctrl+C to stop all nodes"
while ($true) { Start-Sleep -Seconds 1 }
