@echo off
echo ========================================================
echo PULIZIA E RIMOZIONE CLUSTER
echo ========================================================

:: 1. Eliminazione del cluster Kind
echo [1/2] Eliminazione del cluster Kind...
:: Questo comando distrugge il cluster, i nodi e rimuove le configurazioni generate
kind delete cluster

:: 2. Rimozione delle immagini Docker locali
echo [2/2] Rimozione immagini Docker create...
:: Rimuove le immagini v1 costruite precedentemente per liberare spazio
docker rmi alertsystem:v1 alertnotifier:v1 datacollector:v1 usermanager:v1

echo ========================================================
echo PULIZIA COMPLETATA
echo ========================================================
pause