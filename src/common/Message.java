package common;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Classe de message échangée entre les nœuds du système Map-Reduce.
 * Implémente Serializable pour être transmise via ObjectOutputStream sur les sockets.
 */
public class Message implements Serializable {

    private static final long serialVersionUID = 1L;

    private final MessageType type;
    private String data;
    private Map<String, Integer> wordCounts;
    private long offset = 0;
    private long length = -1;

    /** Temps de traitement mesuré par le worker (ms). -1 si non renseigné. */
    private long processingTimeMs = -1;

    /** Nombre total de mots comptés (avec doublons). -1 si non renseigné. */
    private long totalWords = -1;

    /** Nombre de partitions de réduction (utilisé dynamiquement). */
    private int nbReduces = 0;

    // -------------------------------------------------------------------------
    // Constructeurs
    // -------------------------------------------------------------------------

    public Message(MessageType type) {
        this.type = type;
    }

    public Message(MessageType type, String data) {
        this.type = type;
        this.data = data;
    }

    public Message(MessageType type, Map<String, Integer> wordCounts) {
        this.type = type;
        this.wordCounts = new HashMap<>(wordCounts);
    }

    /** Pour MAP_START avec découpage (offset + length) et configuration dynamique. */
    public Message(MessageType type, String data, long offset, long length, int nbReduces) {
        this.type = type;
        this.data = data;
        this.offset = offset;
        this.length = length;
        this.nbReduces = nbReduces;
    }

    /** Pour MAP_SUCCESS avec métriques. */
    public Message(MessageType type, long totalWords, long processingTimeMs) {
        this.type = type;
        this.totalWords = totalWords;
        this.processingTimeMs = processingTimeMs;
    }

    /** Pour REDUCE_SUCCESS avec métriques. */
    public Message(MessageType type, Map<String, Integer> wordCounts, long processingTimeMs) {
        this.type = type;
        this.wordCounts = new HashMap<>(wordCounts);
        this.processingTimeMs = processingTimeMs;
    }

    // -------------------------------------------------------------------------
    // Getters
    // -------------------------------------------------------------------------

    public MessageType getType()                    { return type; }
    public String getData()                         { return data; }
    public Map<String, Integer> getWordCounts()     { return wordCounts; }
    public long getOffset()                         { return offset; }
    public long getLength()                         { return length; }
    public long getProcessingTimeMs()               { return processingTimeMs; }
    public long getTotalWords()                     { return totalWords; }
    public int getNbReduces()                       { return nbReduces; }

    @Override
    public String toString() {
        return "Message{type=" + type + ", data='" + data + "', offset=" + offset + ", length=" + length + "}";
    }
}
