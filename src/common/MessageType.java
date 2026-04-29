package common;

/**
 * Types de messages échangés entre les nœuds du système Map-Reduce.
 */
public enum MessageType {

    /** COORD → MAP : "Lance le comptage sur ce fichier." data = chemin du fichier. */
    MAP_START,

    /** MAP → COORD : "J'ai terminé mon comptage." */
    MAP_SUCCESS,

    /** REDUCE → MAP : "Donne-moi les mots dont je suis responsable." data = WORKER_ID. */
    REDUCE_FETCH,

    /** MAP → REDUCE : sous-ensemble des mots pour ce ReduceWorker. wordCounts = données. */
    MAP_DATA,

    /** COORD → REDUCE : "Commence la réduction." data = adresses des MapWorkers. */
    REDUCE_START,

    /** REDUCE → COORD : "Ma réduction est terminée." wordCounts = résultat final. */
    REDUCE_SUCCESS,

    /** WORKER → COORD : "Je suis prêt." data = identifiant du worker. */
    READY,

    /** COORD → WORKER : "Tu peux t'arrêter." */
    SHUTDOWN,

    /** Générique : un problème est survenu. data = message d'erreur. */
    ERROR
}
