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

    /** Type du message, défini dans Protocol */
    private final int type;

    /** Données textuelles (ex: nom de fichier, identifiant du worker) */
    private String data;

    /** Résultats intermédiaires : mot -> nombre d'occurrences */
    private Map<String, Integer> wordCounts;

    /**
     * Constructeur simple (sans données additionnelles).
     */
    public Message(int type) {
        this.type = type;
    }

    /**
     * Constructeur avec une donnée textuelle (ex: nom de fichier).
     */
    public Message(int type, String data) {
        this.type = type;
        this.data = data;
    }

    /**
     * Constructeur avec un dictionnaire de comptages.
     */
    public Message(int type, Map<String, Integer> wordCounts) {
        this.type = type;
        this.wordCounts = new HashMap<>(wordCounts);
    }

    public int getType() {
        return type;
    }

    public String getData() {
        return data;
    }

    public Map<String, Integer> getWordCounts() {
        return wordCounts;
    }

    @Override
    public String toString() {
        return "Message{type=" + type + ", data='" + data + "', wordCounts=" + wordCounts + "}";
    }
}
