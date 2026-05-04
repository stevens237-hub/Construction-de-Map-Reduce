# run.ps1 — Equivalent du Makefile pour Windows PowerShell
# Usage : .\run.ps1 [run|stop|clean|help]

param(
    [string]$Command = "run"
)

$TEXTS_DIR = ".\texts"

switch ($Command) {

    "run" {
        # Calcul de la taille totale du dossier texts en KB
        $sizeBytes = (Get-ChildItem -Recurse -File $TEXTS_DIR -ErrorAction SilentlyContinue |
                      Measure-Object -Property Length -Sum).Sum
        if (-not $sizeBytes) { $sizeBytes = 0 }
        $sizeKB = [math]::Floor($sizeBytes / 1024)

        Write-Host "--- Analyse des données ---"
        Write-Host "Dossier : $TEXTS_DIR"
        Write-Host "Taille totale : $sizeKB KB"

        if ($sizeKB -lt 1024) {
            $MAPS = 2; $REDS = 1
            Write-Host "Niveau : Petit (< 1 Mo)"
        } elseif ($sizeKB -lt 102400) {
            $MAPS = 4; $REDS = 2
            Write-Host "Niveau : Moyen (1 Mo - 100 Mo)"
        } elseif ($sizeKB -lt 512000) {
            $MAPS = 8; $REDS = 4
            Write-Host "Niveau : Important (100 Mo - 500 Mo)"
        } else {
            $MAPS = 12; $REDS = 6
            Write-Host "Niveau : Massif (> 500 Mo)"
        }

        Write-Host "Lancement avec $MAPS MapWorkers et $REDS ReduceWorkers..."
        Write-Host "---------------------------"

        $env:NB_MAPS    = $MAPS
        $env:NB_REDUCES = $REDS
        docker compose up --build `
            --scale map-worker=$MAPS `
            --scale reduce-worker=$REDS `
            --abort-on-container-exit `
            --exit-code-from coordinator
    }

    "stop" {
        docker compose down
    }

    "clean" {
        docker compose down -v
        Remove-Item -Path ".\output\*" -Recurse -Force -ErrorAction SilentlyContinue
        Write-Host "Nettoyage terminé."
    }

    "help" {
        Write-Host "Commandes disponibles :"
        Write-Host "  .\run.ps1        - Lance le système (run par défaut)"
        Write-Host "  .\run.ps1 run    - Détecte la taille des données et lance le système"
        Write-Host "  .\run.ps1 stop   - Arrête tous les conteneurs"
        Write-Host "  .\run.ps1 clean  - Arrête les conteneurs et supprime les fichiers de sortie"
    }

    default {
        Write-Host "Commande inconnue : '$Command'"
        Write-Host "Utilisez .\run.ps1 help pour voir les commandes disponibles."
        exit 1
    }
}
