# Makefile pour l'automatisation du système Map-Reduce distribué

SHELL := /bin/bash
TEXTS_DIR := ./texts

.PHONY: run stop clean help

help:
	@echo "Commandes disponibles :"
	@echo "  make run   - Détecte la taille des données et lance le système avec le nombre optimal de workers"
	@echo "  make stop  - Arrête tous les conteneurs"
	@echo "  make clean - Arrête les conteneurs et supprime les volumes et les fichiers de sortie"

run:
	@# 1. Calcul de la taille en Kilo-octets (plus robuste avec awk)
	@SIZE_KB=$$(du -sk $(TEXTS_DIR) | awk '{print $$1}'); \
	echo "--- Analyse des données ---"; \
	echo "Dossier : $(TEXTS_DIR)"; \
	echo "Taille totale : $$SIZE_KB KB"; \
	\
	# 2. Logique de décision pour le nombre de workers \
	if [ $$SIZE_KB -lt 1024 ]; then \
		MAPS=2; REDS=1; \
		echo "Niveau : Petit (< 1 Mo)"; \
	elif [ $$SIZE_KB -lt 102400 ]; then \
		MAPS=4; REDS=2; \
		echo "Niveau : Moyen (1 Mo - 100 Mo)"; \
	elif [ $$SIZE_KB -lt 512000 ]; then \
		MAPS=8; REDS=4; \
		echo "Niveau : Important (100 Mo - 500 Mo)"; \
	else \
		MAPS=12; REDS=6; \
		echo "Niveau : Massif (> 500 Mo)"; \
	fi; \
	\
	# 3. Lancement de Docker Compose avec export explicite et arrêt automatique \
	echo "Lancement avec $$MAPS MapWorkers et $$REDS ReduceWorkers..."; \
	echo "---------------------------"; \
	export NB_MAPS=$$MAPS; \
	export NB_REDUCES=$$REDS; \
	docker compose up --build --scale map-worker=$$MAPS --scale reduce-worker=$$REDS --abort-on-container-exit --exit-code-from coordinator

stop:
	docker compose down

clean:
	docker compose down -v
	rm -rf output/*
	@echo "Nettoyage terminé."
