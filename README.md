# Map-Reduce — Compteur de mots distribué

Plateforme distribuée de comptage de mots inspirée du modèle Hadoop/Map-Reduce,
implémentée en Java pur avec des **Sockets TCP** et de la **sérialisation d'objets**.
Le déploiement se fait via **Docker Compose** sur un réseau bridge simulant un
environnement multi-machines.

---

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                    COORDINATOR                       │
│  - découvre les fichiers texte                       │
│  - distribue les tâches MAP                          │
│  - déclenche les tâches REDUCE                       │
│  - agrège le résultat final et l'affiche             │
└──────────┬──────────────────────────┬───────────────┘
           │ MAP_START                │ REDUCE_START
           ▼                          ▼
┌──────────────────┐        ┌──────────────────────┐
│  MapWorker 1     │        │  ReduceWorker 1       │
│  (fichier1.txt)  │◄───────│  (mots partitions 0)  │
└──────────────────┘REDUCE  └──────────────────────┘
┌──────────────────┐FETCH   ┌──────────────────────┐
│  MapWorker 2     │◄───────│  ReduceWorker 2       │
│  (fichier2.txt)  │        │  (mots partitions 1)  │
└──────────────────┘        └──────────────────────┘
```

### Composants

| Composant       | Rôle                                                                 |
|-----------------|----------------------------------------------------------------------|
| **Coordinator** | Orchestre toutes les phases ; point d'entrée unique                  |
| **MapWorker**   | Reçoit un fichier, compte les mots, répond aux requêtes des reduces  |
| **ReduceWorker**| Collecte les sous-ensembles de mots, agrège les comptages            |

### Protocole de messages (`common/`)

| Message          | Émetteur → Destinataire | Contenu                                   |
|------------------|-------------------------|-------------------------------------------|
| `MAP_START`      | Coordinator → Map       | Chemin du fichier à traiter               |
| `MAP_SUCCESS`    | Map → Coordinator       | Confirmation + statistiques               |
| `MAP_FAILURE`    | Map → Coordinator       | Message d'erreur                          |
| `REDUCE_START`   | Coordinator → Reduce    | Liste CSV `host:port` des MapWorkers      |
| `REDUCE_FETCH`   | Reduce → Map            | `workerId` du reduce (pour la partition)  |
| `MAP_DATA`       | Map → Reduce            | Sous-dictionnaire `mot → occurrences`     |
| `REDUCE_SUCCESS` | Reduce → Coordinator    | Dictionnaire final agrégé                 |
| `SHUTDOWN`       | Coordinator → Workers   | Ordre d'arrêt propre                      |

### Partitionnement des mots

Chaque mot est assigné à un ReduceWorker de manière déterministe :

```java
int workerId = Math.floorMod(word.toLowerCase().hashCode(), nbReduces);
```

`Math.floorMod` est utilisé (et non `%`) pour garantir un résultat positif même
quand `hashCode()` est négatif.

---

## Prérequis

- **Docker** ≥ 20.x
- **Docker Compose** ≥ 2.x
- (Optionnel pour le mode local) **JDK** ≥ 17

---

## Démarrage rapide

### Avec Docker Compose

```bash
# 1. Cloner / décompresser le projet
cd Construction-de-Map-Reduce

# 2. Lancer tous les conteneurs
docker compose up --build

# 3. Consulter les logs du coordinator pour voir le résultat
docker compose logs coordinator
```

Le résultat final ressemble à :

```
╔══════════════════════════════════════════╗
║       RÉSULTAT FINAL — Map-Reduce        ║
╚══════════════════════════════════════════╝
  Temps total : 842 ms
  Mots distincts : 47

  le                   : 15
  la                   : 8
  et                   : 7
  dans                 : 5
  ...
```

### Sans Docker (mode local multi-processus)

```bash
# Compiler
mkdir -p classes
find src -name "*.java" | xargs javac -d classes

# Terminal 1 — MapWorker sur port 5000
NB_REDUCES=2 java -cp classes mapworker.MapWorker

# Terminal 2 — ReduceWorker 0 sur port 6000
WORKER_ID=0 NB_REDUCES=2 java -cp classes reduceworker.ReduceWorker

# Terminal 3 — ReduceWorker 1 sur port 6000  ← changer le port si même machine !
WORKER_ID=1 NB_REDUCES=2 java -cp classes reduceworker.ReduceWorker

# Terminal 4 — Coordinator
MAP_WORKERS=localhost:5000 \
REDUCE_WORKERS=localhost:6000,localhost:6001 \
NB_REDUCES=2 \
TEXTS_DIR=./texts \
java -cp classes coordinator.Coordinator
```

> **Note** : Sur une seule machine, les ReduceWorkers doivent écouter sur des
> ports différents. En Docker, chaque conteneur a sa propre IP et peut tous
> écouter sur le même port (6000).

---

## Configuration par variables d'environnement

| Variable        | Défaut                            | Description                                    |
|-----------------|-----------------------------------|------------------------------------------------|
| `MAP_WORKERS`   | `localhost:5000,localhost:5001`   | Liste CSV `host:port` des MapWorkers           |
| `REDUCE_WORKERS`| `localhost:6000,localhost:6001`   | Liste CSV `host:port` des ReduceWorkers        |
| `NB_REDUCES`    | `2`                               | Nombre de ReduceWorkers (doit être cohérent)   |
| `TEXTS_DIR`     | `./texts`                         | Répertoire contenant les fichiers `.txt`       |
| `TIMEOUT_MS`    | `15000`                           | Timeout socket en millisecondes                |
| `WORKER_ID`     | `0`                               | Identifiant du ReduceWorker (0, 1, 2…)         |

---

## Structure du projet

```
.
├── docker-compose.yml
├── texts/
│   ├── fichier1.txt
│   ├── fichier2.txt
│   └── fichier3.txt
└── src/
    ├── common/
    │   ├── Message.java          ← objet sérialisable échangé sur les sockets
    │   ├── MessageType.java      ← enum de tous les types de messages
    │   └── Protocol.java         ← ports, timeout, fonction de partitionnement
    ├── coordinator/
    │   ├── Coordinator.java      ← orchestrateur central
    │   └── Dockerfile
    ├── mapworker/
    │   ├── MapWorker.java        ← tâche MAP
    │   └── Dockerfile
    └── reduceworker/
        ├── ReduceWorker.java     ← tâche REDUCE
        └── Dockerfile
```

---

## Phases d'exécution détaillées

### Phase 0 — Attente des workers
Le Coordinator tente de se connecter à chaque worker jusqu'à `CONNECT_RETRIES=10` fois
avec un délai de 2 secondes entre chaque tentative. Cela permet aux conteneurs Docker
de démarrer dans n'importe quel ordre.

### Phase 1 — MAP
1. Le Coordinator liste tous les `.txt` du dossier `TEXTS_DIR`.
2. Les fichiers sont distribués en **round-robin** sur les MapWorkers.
3. Chaque worker reçoit `MAP_START(filePath)`, compte les mots, renvoie `MAP_SUCCESS`.
4. Les workers MAP sont sollicités **en parallèle** (un thread par worker).
5. Si un worker renvoie `MAP_FAILURE`, la phase échoue et le système s'arrête.

### Phase 2 — REDUCE
1. Le Coordinator envoie `REDUCE_START(mapWorkersCsv)` à tous les ReduceWorkers,
   **en parallèle**.
2. Chaque ReduceWorker contacte **tous** les MapWorkers avec `REDUCE_FETCH(workerId)`.
3. Chaque MapWorker filtre ses comptages locaux pour ne renvoyer que les mots dont
   `partitionWord(mot, nbReduces) == workerId`.
4. Le ReduceWorker agrège les comptages reçus (somme des occurrences par mot).
5. Il renvoie `REDUCE_SUCCESS(dictionnaire)` au Coordinator.

### Phase 3 — Agrégation finale
Le Coordinator fusionne les dictionnaires de tous les ReduceWorkers et affiche
le résultat trié par fréquence décroissante.

### Phase 4 — Shutdown
Le Coordinator envoie `SHUTDOWN` à tous les workers pour qu'ils s'arrêtent proprement.

---

## Tolérance aux pannes

| Scénario                          | Comportement actuel                                          |
|-----------------------------------|--------------------------------------------------------------|
| MapWorker inaccessible (MAP)      | `MAP_FAILURE` → arrêt de la plateforme                       |
| MapWorker inaccessible (REDUCE)   | Le ReduceWorker logue un WARNING et continue sans lui        |
| ReduceWorker inaccessible         | Le Coordinator logue une erreur ; les autres reduces avancent|
| Fichier introuvable               | `MAP_FAILURE` envoyé au Coordinator                          |

Pour aller plus loin en tolérance aux pannes :
- **Ré-assignation de fichier** : si un MapWorker échoue en phase MAP, relancer
  le fichier sur un autre worker disponible.
- **Exécution spéculative** : lancer le même calcul sur deux workers en parallèle
  et utiliser le premier résultat arrivé (comme Hadoop).
- **Heartbeat** : thread de monitoring qui détecte les workers silencieux et
  les remplace à chaud.

---

## Tester la scalabilité

Ajoutez des workers dans `docker-compose.yml` et augmentez les variables
d'environnement :

```yaml
# Ajouter un 3e MapWorker
map-worker-3:
  <<: *map-worker-base
  container_name: map-worker-3
  hostname: map-worker-3
  environment:
    - NB_REDUCES=3

# Mettre à jour le Coordinator
coordinator:
  environment:
    - MAP_WORKERS=map-worker-1:5000,map-worker-2:5000,map-worker-3:5000
    - REDUCE_WORKERS=reduce-worker-1:6000,reduce-worker-2:6000,reduce-worker-3:6000
    - NB_REDUCES=3
```

Mesurez le temps affiché dans la sortie du Coordinator pour comparer les
configurations 1/2/3/4 workers MAP et REDUCE.

---

## Pistes d'amélioration

- **RMI** : remplacer les sockets bruts par Java RMI pour une API plus orientée objet.
- **Compression** : compresser les messages `MAP_DATA` (GZIPOutputStream) pour
  réduire le trafic réseau sur de gros volumes.
- **Pool de connexions** : réutiliser les connexions TCP entre le ReduceWorker
  et les MapWorkers plutôt d'en ouvrir une par requête.
- **Interface web** : exposer un endpoint HTTP dans le Coordinator pour suivre
  l'avancement en temps réel.
- **Fichiers volumineux** : lire les fichiers en streaming (`BufferedReader`) et
  traiter ligne par ligne pour limiter l'empreinte mémoire.
