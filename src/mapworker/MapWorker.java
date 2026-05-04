package mapworker;

import common.Message;
import common.MessageType;
import common.Protocol;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class MapWorker {

    private static final Map<String, Integer> currentCounts = new ConcurrentHashMap<>();
    private static final AtomicLong totalWordsProcessed = new AtomicLong(0);
    private static volatile boolean running = true;
    private static int nbReduces;

    public static void main(String[] args) {
        String nbReducesStr    = System.getenv("NB_REDUCES");
        String coordinatorHost = System.getenv("COORDINATOR_HOST");
        String hostname        = System.getenv("HOSTNAME");

        if (nbReducesStr == null || coordinatorHost == null) {
            System.err.println("Missing env vars: NB_REDUCES, COORDINATOR_HOST");
            return;
        }
        nbReduces = Integer.parseInt(nbReducesStr);
        String workerId = hostname != null ? hostname : "map-worker";

        try (ServerSocket serverSocket = new ServerSocket(Protocol.MAP_WORKER_PORT)) {
            System.out.println("MapWorker " + workerId + " listening on port " + Protocol.MAP_WORKER_PORT);

            notifyReady(coordinatorHost, workerId);

            while (running) {
                Socket clientSocket = serverSocket.accept();
                new Thread(() -> handleClient(clientSocket)).start();
            }
        } catch (IOException e) {
            if (running) e.printStackTrace();
        }
        System.out.println("MapWorker " + workerId + " stopped.");
    }

    // -------------------------------------------------------------------------
    // Signal de disponibilité au coordinator
    // -------------------------------------------------------------------------

    private static void notifyReady(String coordinatorHost, String workerId) {
        for (int attempt = 0; attempt < 20; attempt++) {
            try {
                if (attempt > 0) Thread.sleep(1000);
                try (Socket s = new Socket(coordinatorHost, Protocol.COORDINATOR_READY_PORT);
                     ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream())) {
                    s.setSoTimeout(Protocol.TIMEOUT_MS);
                    out.writeObject(new Message(MessageType.READY, workerId));
                    out.flush();
                }
                System.out.println("READY sent to coordinator.");
                return;
            } catch (Exception e) {
                System.out.println("Waiting for coordinator... attempt " + (attempt + 1));
            }
        }
        System.err.println("Could not reach coordinator to send READY.");
    }

    // -------------------------------------------------------------------------
    // Gestion des connexions entrantes
    // -------------------------------------------------------------------------

    private static void handleClient(Socket clientSocket) {
        try {
            clientSocket.setSoTimeout(Protocol.TIMEOUT_MS);
            ObjectOutputStream out = new ObjectOutputStream(clientSocket.getOutputStream());
            ObjectInputStream  in  = new ObjectInputStream(clientSocket.getInputStream());

            Message request = (Message) in.readObject();
            System.out.println("MapWorker received: " + request.getType());

            switch (request.getType()) {
                case MAP_START:
                    long start = System.currentTimeMillis();
                    long wordsInSplit = processFile(request.getData(), request.getOffset(), request.getLength());
                    long elapsed = System.currentTimeMillis() - start;
                    // MAP_SUCCESS transporte le nombre de mots et le temps de traitement
                    out.writeObject(new Message(MessageType.MAP_SUCCESS, wordsInSplit, elapsed));
                    out.flush();
                    break;

                case REDUCE_FETCH:
                    int reducerId = Integer.parseInt(request.getData());
                    Map<String, Integer> subMap = new HashMap<>();
                    for (Map.Entry<String, Integer> entry : currentCounts.entrySet()) {
                        if (Protocol.getReduceWorkerForWord(entry.getKey(), nbReduces) == reducerId) {
                            subMap.put(entry.getKey(), entry.getValue());
                        }
                    }
                    out.writeObject(new Message(MessageType.MAP_DATA, subMap));
                    out.flush();
                    break;

                case SHUTDOWN:
                    running = false;
                    break;

                default:
                    System.out.println("Unknown message type: " + request.getType());
            }
        } catch (IOException | ClassNotFoundException e) {
            if (running) e.printStackTrace();
        } finally {
            try { clientSocket.close(); } catch (IOException e) { /* ignore */ }
        }
    }

    // -------------------------------------------------------------------------
    // Lecture du fichier avec gestion des blocs (offset + length)
    // Retourne le nombre total de mots comptés (avec doublons)
    // -------------------------------------------------------------------------

    private static long processFile(String filePath, long offset, long length) {
        System.out.println("Processing " + filePath + " [offset=" + offset + ", length=" + length + "]");
        long wordCount = 0;
        try (FileChannel channel = FileChannel.open(Paths.get(filePath), StandardOpenOption.READ)) {
            channel.position(offset);

            ByteBuffer buf = ByteBuffer.allocate(65536);
            StringBuilder word = new StringBuilder();
            long pos = offset;
            long end = offset + length;
            boolean skipPartialWord = offset > 0;

            loop:
            while (channel.read(buf) > 0) {
                buf.flip();
                while (buf.hasRemaining()) {
                    char c = (char) (buf.get() & 0xFF);
                    pos++;

                    if (skipPartialWord) {
                        if (Character.isWhitespace(c)) skipPartialWord = false;
                        continue;
                    }

                    if (Character.isLetterOrDigit(c)) {
                        word.append(c);
                    } else {
                        if (word.length() > 0) {
                            currentCounts.merge(word.toString().toLowerCase(), 1, Integer::sum);
                            wordCount++;
                            word.setLength(0);
                        }
                        if (pos > end) break loop;
                    }
                }
                buf.clear();
            }
            if (!skipPartialWord && word.length() > 0) {
                currentCounts.merge(word.toString().toLowerCase(), 1, Integer::sum);
                wordCount++;
            }

            totalWordsProcessed.addAndGet(wordCount);
            System.out.println("Done. Words in split: " + wordCount
                    + " | Unique words total: " + currentCounts.size());
        } catch (IOException e) {
            System.err.println("Error reading " + filePath + ": " + e.getMessage());
        }
        return wordCount;
    }
}
