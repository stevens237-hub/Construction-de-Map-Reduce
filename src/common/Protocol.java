package common;

/**
 * Constantes partagées du protocole Map-Reduce (ports, timeouts, partitionnement).
 * Les types de messages sont définis dans MessageType.
 */
public class Protocol {

    // -------------------------------------------------------------------------
    // Ports d'écoute des workers
    // -------------------------------------------------------------------------

    /** Port sur lequel chaque MapWorker attend les ordres du Coordinator. */
    public static final int MAP_WORKER_PORT = 5000;

    /** Port sur lequel chaque ReduceWorker attend les requêtes. */
    public static final int REDUCE_WORKER_PORT = 6000;

    /** Port sur lequel le Coordinator attend les signaux READY des workers. */
    public static final int COORDINATOR_READY_PORT = 7000;

    // -------------------------------------------------------------------------
    // Timeouts et limites
    // -------------------------------------------------------------------------

    /** Délai maximum (ms) pour qu'un worker réponde avant d'être considéré en panne. */
    public static final int TIMEOUT_MS = 10_000;

    /** Nombre maximum de tentatives en cas d'échec d'une tâche map. */
    public static final int MAX_RETRIES = 3;

    // -------------------------------------------------------------------------
    // Utilitaire de partitionnement
    // -------------------------------------------------------------------------

    /**
     * Détermine quel ReduceWorker est responsable d'un mot donné,
     * en utilisant un hash modulo le nombre de reduces.
     */
    public static int getReduceWorkerForWord(String word, int nbReduces) {
        return Math.abs(word.hashCode() % nbReduces);
    }
}
