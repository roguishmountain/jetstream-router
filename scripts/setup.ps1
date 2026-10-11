# Starts everything needed to run jetstream-router on minikube, from nothing
# running: starts the cluster if needed, builds and loads the image, deploys,
# and waits until every pod is ready. Safe to run again at any time; a second
# run rebuilds the image and restarts the pods onto it.
#
# Run from anywhere:
#   powershell -ExecutionPolicy Bypass -File scripts\setup.ps1
#
# Docker Desktop must already be running. The script doesn't delete anything.

$Namespace = "jetstream-router"
$Image = "jetstream-router:0.1"

# Native commands don't stop the script when they fail, so check each one.
function Invoke-Step {
    param([string]$Description, [scriptblock]$Command)
    Write-Host ""
    Write-Host "==> $Description" -ForegroundColor Cyan
    & $Command
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Failed: $Description (exit code $LASTEXITCODE)" -ForegroundColor Red
        exit 1
    }
}

# Run from the repository root (this script's parent folder), so the build
# and k8s/ paths work wherever the script is started from.
Set-Location (Split-Path -Parent $PSScriptRoot)

# 1. Docker must be running: minikube runs inside it.
docker info *> $null
if ($LASTEXITCODE -ne 0) {
    Write-Host "Docker isn't running. Start Docker Desktop, wait until it says it's running, then run this again." -ForegroundColor Red
    exit 1
}

# 2. Start minikube unless it's already running.
minikube status *> $null
if ($LASTEXITCODE -eq 0) {
    Write-Host "minikube is already running."
} else {
    Invoke-Step "Starting minikube" { minikube start --driver=docker }
}

# minikube can report success while Kubernetes inside it isn't answering.
kubectl get nodes *> $null
if ($LASTEXITCODE -ne 0) {
    Write-Host "minikube started, but Kubernetes isn't responding." -ForegroundColor Red
    Write-Host "Recreate the cluster with 'minikube delete', then run this script again."
    exit 1
}

# 3. Memory figures for 'kubectl top'. Harmless if already enabled.
Invoke-Step "Enabling metrics-server" { minikube addons enable metrics-server }

# 4. Build the image and copy it into minikube, which can't see Docker
#    Desktop's images.
Invoke-Step "Building $Image" { docker build -t $Image . }
Invoke-Step "Loading $Image into minikube" { minikube image load $Image }

# 5. Deploy. If the app was already deployed, restart its pods so they pick
#    up the image just loaded; a fresh deploy starts on it anyway.
kubectl get namespace $Namespace *> $null
$alreadyDeployed = ($LASTEXITCODE -eq 0)

Invoke-Step "Applying manifests" { kubectl apply -f k8s/ }

if ($alreadyDeployed) {
    Invoke-Step "Restarting the router and workers onto the new image" {
        kubectl rollout restart -n $Namespace deployment
    }
}

# 6. Wait for every Deployment to finish rolling out and Kafka to be ready.
#    A first start downloads Kafka's image, so allow plenty of time.
Invoke-Step "Waiting for Kafka" {
    kubectl rollout status -n $Namespace statefulset/kafka --timeout=10m
}
foreach ($name in "router", "post-worker", "engagement-worker", "follow-worker", "delete-worker") {
    Invoke-Step "Waiting for $name" {
        kubectl rollout status -n $Namespace "deployment/$name" --timeout=10m
    }
}

Write-Host ""
Write-Host "Everything is running:" -ForegroundColor Green
kubectl get pods -n $Namespace
Write-Host ""
Write-Host "Follow the router:  kubectl logs -n $Namespace deploy/router -f"
Write-Host "Memory per pod:     kubectl top pod -n $Namespace   (may take a minute after first enabling metrics-server)"
