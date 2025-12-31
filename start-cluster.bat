@echo off
echo ========================================================
echo AVVIO CONFIGURAZIONE CLUSTER KUBERNETES
echo ========================================================

:: 1. Creazione del cluster
echo [1/8] Creazione del cluster Kind...
kind create cluster --config kind-config.yaml
if %errorlevel% neq 0 exit /b %errorlevel%

:: 2. Installazione Ingress Nginx
echo [2/8] Installazione Ingress Nginx Controller...
kubectl apply -f https://raw.githubusercontent.com/kubernetes/ingress-nginx/main/deploy/static/provider/kind/deploy.yaml

:: Attesa che il pod del controller sia pronto (sostituisce il semplice 'get pods')
echo Attesa avvio Ingress Controller...
kubectl wait --namespace ingress-nginx --for=condition=ready pod --selector=app.kubernetes.io/component=controller --timeout=90s

:: 3. Build delle immagini Docker
echo [3/8] Building delle immagini Docker...
docker build -t alertsystem:v1 -f AlertSystem/Dockerfile .
docker build -t alertnotifier:v1 -f AlertNotifierSystem/Dockerfile .
docker build -t datacollector:v1 -f DataCollector/Dockerfile .
docker build -t usermanager:v1 -f UserManager/Dockerfile .

:: 4. Caricamento immagini nel cluster Kind
echo [4/8] Caricamento immagini nel cluster Kind...
kind load docker-image alertsystem:v1
kind load docker-image alertnotifier:v1
kind load docker-image datacollector:v1
kind load docker-image usermanager:v1

:: 5. Creazione Secret
echo [5/8] Creazione Secret da file .env...
kubectl create secret generic app-secrets --from-env-file=.env

:: 6. Applicazione dei manifesti infrastrutturali (DB, Kafka)
echo [6/8] Deploy DB e Kafka...
kubectl apply -f db-kafka.yaml

:: 7. Applicazione dei microservizi
echo [7/8] Deploy Microservizi e Monitoring...
kubectl apply -f microservices.yaml
kubectl apply -f prometheus-k8s.yaml

:: 8. Configurazione Network e Ingress
echo [8/8] Applicazione regole di Network e Ingress...
kubectl apply -f ingress.yaml
kubectl apply -f network.yaml

echo ========================================================
echo INSTALLAZIONE COMPLETATA CON SUCCESSO!
echo ========================================================
pause