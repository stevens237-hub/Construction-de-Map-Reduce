package common;

/**
 * Définit le protocole de communication entre les nœuds du système Map-Reduce.
 * Centralise toutes les constantes partagées (ports, types de messages).
 */
public class Protocol {

    // -------------------------------------------------------------------------
    // Ports d'écoute des workers
    // -------------------------------------------------------------------------

    /** Port sur lequel chaque MapWorker attend les ordres du Coordinateur. */
    public static final int MAP_WORKER_PORT = 5000;

    /** Port sur lequel chaque ReduceWorker attend les requêtes des MapWorkers. */
    public static final int REDUCE_WORKER_PORT = 6000;

    // -------------------------------------------------------------------------
    // Types de messages
    // -------------------------------------------------------------------------

    /**
     * COORD → MAP : "Lance le comptage sur ce fichier."
     * data = chemin vers le fichier à traiter.
     */
    public static final int MAP_START = 1;

    /**
     * MAP → COORD : "J'ai terminé mon comptage."
     */
    public static final int MAP_SUCCESS = 2;

    /**
     * REDUCE → MAP : "Donne-moi les mots dont je suis responsable."
     * data = identifiant du ReduceWorker (son WORKER_ID).
     */
    public static final int REDUCE_FETCH = 3;

    /**
     * MAP → REDUCE : Réponse à REDUCE_FETCH.
     * wordCounts = sous-ensemble des mots correspondant à ce ReduceWorker.
     */
    public static final int MAP_DATA = 4;

    /**
     * COORD → REDUCE : "Commence la phase de réduction."
     * data = liste des adresses des MapWorkers (à contacter pour récupérer les données).
     */
    public static final int REDUCE_START = 5;

    /**
     * REDUCE → COORD : "Ma réduction est terminée."
     * wordCounts = dictionnaire final consolidé.
     */
    public static final int REDUCE_SUCCESS = 6;

    /**
     * Générique : un problème est survenu.
     * data = message d'erreur.
     */
    public static final int ERROR = -1;

    // -------------------------------------------------------------------------
    // Utilitaire de partitionnement
    // -------------------------------------------------------------------------

    /**
     * Détermine quel ReduceWorker est responsable d'un mot donné,
     * en utilisant un simple hash modulo le nombre de reduces.
     *
     * @param word      Le mot à assigner.
     * @param nbReduces Le nombre total de ReduceWorkers.
     * @return L'identifiant (WORKER_ID) du ReduceWorker responsable.
     */
    public static int getReduceWorkerForWord(String word, int nbReduces) {
        return Math.abs(word.hashCode() % nbReduces);
    }
}
