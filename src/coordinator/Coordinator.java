package coordinator;

import common.Message;
import common.MessageType;
import common.Protocol;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class Coordinator {

    private static class FileSplit {
        final String path;
        final long offset;
        final long length;

        FileSplit(String path, long offset, long length) {
            this.path = path;
            this.offset = offset;
            this.length = length;
        }

        @Override
        public String toString() {
            return Paths.get(path).getFileName() + " [" + offset + ", " + (offset + length) + "]";
        }
    }

    public static void main(String[] args) {
        System.out.println("Coordinator started");

        String mapWorkersEnv   = System.getenv("MAP_WORKERS");
        String reduceWorkersEnv = System.getenv("REDUCE_WORKERS");
        String textsDir        = System.getenv("TEXTS_DIR");

        if (mapWorkersEnv == null || reduceWorkersEnv == null || textsDir == null) {
            System.err.println("Missing env vars: MAP_WORKERS, REDUCE_WORKERS, TEXTS_DIR");
            return;
        }

        String[] mapWorkers    = mapWorkersEnv.split(",");
        String[] reduceWorkers = reduceWorkersEnv.split(",");
        int totalWorkers       = mapWorkers.length + reduceWorkers.length;

        // Phase 0 — attendre que tous les workers soient prêts
        System.out.println("Waiting for " + totalWorkers + " workers to be ready...");
        if (!waitForWorkers(totalWorkers)) {
            System.err.println("Timeout waiting for workers. Aborting.");
            return;
        }
        System.out.println("All workers ready.");

        // Phase 1 — calcul des splits et phase map
        System.out.println("--- Map Phase ---");
        List<FileSplit> splits;
        try {
            splits = computeSplits(textsDir, mapWorkers.length);
        } catch (IOException e) {
            System.err.println("Cannot read texts dir: " + e.getMessage());
            return;
        }

        if (splits.isEmpty()) {
            System.out.println("No text files found in " + textsDir);
            return;
        }
        System.out.println("Total splits to process: " + splits.size() + " sur " + mapWorkers.length + " mappers");

        if (!runMapPhase(splits, mapWorkers)) {
            System.err.println("Map phase completed with errors — results may be incomplete.");
        }

        // Phase 2 — phase reduce
        System.out.println("--- Reduce Phase ---");
        List<Map<String, Integer>> partialResults = runReducePhase(reduceWorkers, mapWorkersEnv);

        // Phase 3 — agrégation finale
        System.out.println("--- Aggregation ---");
        Map<String, Integer> finalMap = new HashMap<>();
        for (Map<String, Integer> part : partialResults) {
            for (Map.Entry<String, Integer> e : part.entrySet()) {
                finalMap.merge(e.getKey(), e.getValue(), Integer::sum);
            }
        }

        System.out.println("Total unique words: " + finalMap.size());
        System.out.println("--- Top 20 Words ---");
        finalMap.entrySet().stream()
                .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                .limit(20)
                .forEach(e -> System.out.println(e.getKey() + ": " + e.getValue()));

        writeOutput(finalMap, textsDir);

        // Shutdown
        System.out.println("--- Shutting down workers ---");
        for (String w : mapWorkers)    sendShutdown(w);
        for (String w : reduceWorkers) sendShutdown(w);

        System.out.println("Coordinator finished.");
    }

    // -------------------------------------------------------------------------
    // Phase 0 : attendre les READY
    // -------------------------------------------------------------------------

    private static boolean waitForWorkers(int total) {
        CountDownLatch latch = new CountDownLatch(total);
        try (ServerSocket ss = new ServerSocket(Protocol.COORDINATOR_READY_PORT)) {
            ss.setSoTimeout(5_000);
            long deadline = System.currentTimeMillis() + 30_000;

            while (latch.getCount() > 0 && System.currentTimeMillis() < deadline) {
                try {
                    Socket client = ss.accept();
                    new Thread(() -> {
                        try (client;
                             ObjectInputStream in = new ObjectInputStream(client.getInputStream())) {
                            Message msg = (Message) in.readObject();
                            if (msg.getType() == MessageType.READY) {
                                System.out.println("  READY: " + msg.getData());
                                latch.countDown();
                            }
                        } catch (Exception e) {
                            System.err.println("Error reading READY: " + e.getMessage());
                        }
                    }).start();
                } catch (SocketTimeoutException e) {
                    // vérifie la deadline et réessaie
                }
            }
        } catch (IOException e) {
            System.err.println("Cannot open ready port: " + e.getMessage());
            return false;
        }
        return latch.getCount() == 0;
    }

    // -------------------------------------------------------------------------
    // Phase 1 : calcul des splits
    // -------------------------------------------------------------------------

    private static List<FileSplit> computeSplits(String textsDir, int nbMappers) throws IOException {
        List<FileSplit> splits = new ArrayList<>();

        List<Path> files = Files.list(Paths.get(textsDir))
                .filter(Files::isRegularFile)
                .filter(p -> p.toString().endsWith(".txt"))
                .collect(Collectors.toList());

        for (Path file : files) {
            long size = Files.size(file);
            if (size == 0) continue;

            // Divise chaque fichier en autant de blocs que de mappers disponibles
            // Garantit que tous les mappers ont du travail quel que soit la taille du fichier
            long blockSize = (size + nbMappers - 1) / nbMappers; // ceil division
            long offset = 0;
            while (offset < size) {
                long length = Math.min(blockSize, size - offset);
                splits.add(new FileSplit(file.toString(), offset, length));
                offset += length;
            }
        }
        return splits;
    }

    // -------------------------------------------------------------------------
    // Phase 1 : exécution des tâches map avec retry
    // -------------------------------------------------------------------------

    private static boolean runMapPhase(List<FileSplit> splits, String[] mapWorkers) {
        AtomicInteger failures = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(splits.size());

        for (int i = 0; i < splits.size(); i++) {
            final FileSplit split = splits.get(i);
            final int idx = i;

            new Thread(() -> {
                boolean success = false;
                for (int retry = 0; retry < Protocol.MAX_RETRIES && !success; retry++) {
                    // Sur retry, essayer un worker différent
                    String worker = mapWorkers[(idx + retry) % mapWorkers.length];
                    success = sendMapTask(worker, split);
                    if (!success && retry < Protocol.MAX_RETRIES - 1) {
                        System.err.println("Retry " + (retry + 1) + " for " + split);
                    }
                }
                if (!success) {
                    System.err.println("Failed after " + Protocol.MAX_RETRIES + " retries: " + split);
                    failures.incrementAndGet();
                }
                latch.countDown();
            }).start();
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return failures.get() == 0;
    }

    private static boolean sendMapTask(String worker, FileSplit split) {
        String[] parts = worker.split(":");
        try (Socket socket = new Socket(parts[0], Integer.parseInt(parts[1]));
             ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
             ObjectInputStream in   = new ObjectInputStream(socket.getInputStream())) {

            socket.setSoTimeout(Protocol.TIMEOUT_MS);
            out.writeObject(new Message(MessageType.MAP_START, split.path, split.offset, split.length));
            out.flush();

            Message response = (Message) in.readObject();
            if (response.getType() == MessageType.MAP_SUCCESS) {
                System.out.println("Map OK: " + split + " on " + worker);
                return true;
            }
        } catch (Exception e) {
            System.err.println("Map error [" + worker + "]: " + e.getMessage());
        }
        return false;
    }

    // -------------------------------------------------------------------------
    // Phase 2 : exécution des tâches reduce
    // -------------------------------------------------------------------------

    private static List<Map<String, Integer>> runReducePhase(String[] reduceWorkers, String mapWorkersEnv) {
        List<Map<String, Integer>> results = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch latch = new CountDownLatch(reduceWorkers.length);

        for (String worker : reduceWorkers) {
            new Thread(() -> {
                String[] parts = worker.split(":");
                // timeout généreux : le reducer contacte tous les mappers en parallèle
                int reducerTimeout = Protocol.TIMEOUT_MS * mapWorkersEnv.split(",").length * 2;
                try (Socket socket = new Socket(parts[0], Integer.parseInt(parts[1]));
                     ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                     ObjectInputStream in   = new ObjectInputStream(socket.getInputStream())) {

                    socket.setSoTimeout(reducerTimeout);
                    out.writeObject(new Message(MessageType.REDUCE_START, mapWorkersEnv));
                    out.flush();

                    Message response = (Message) in.readObject();
                    if (response.getType() == MessageType.REDUCE_SUCCESS) {
                        results.add(response.getWordCounts());
                        System.out.println("Reduce OK on " + worker);
                    }
                } catch (Exception e) {
                    System.err.println("Reduce error [" + worker + "]: " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            }).start();
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return results;
    }

    // -------------------------------------------------------------------------
    // Écriture du fichier de sortie
    // -------------------------------------------------------------------------

    private static void writeOutput(Map<String, Integer> result, String textsDir) {
        Path outputPath = Paths.get(textsDir, "output.txt");
        try (BufferedWriter writer = Files.newBufferedWriter(outputPath)) {
            result.entrySet().stream()
                    .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                    .forEach(e -> {
                        try {
                            writer.write(e.getKey() + ": " + e.getValue());
                            writer.newLine();
                        } catch (IOException ex) {
                            throw new UncheckedIOException(ex);
                        }
                    });
            System.out.println("Output written to " + outputPath);
        } catch (IOException e) {
            System.err.println("Error writing output: " + e.getMessage());
        }
    }

    // -------------------------------------------------------------------------
    // Shutdown
    // -------------------------------------------------------------------------

    private static void sendShutdown(String worker) {
        String[] parts = worker.split(":");
        try (Socket socket = new Socket(parts[0], Integer.parseInt(parts[1]));
             ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream())) {
            socket.setSoTimeout(Protocol.TIMEOUT_MS);
            out.writeObject(new Message(MessageType.SHUTDOWN));
            out.flush();
        } catch (Exception e) {
            System.err.println("Could not shutdown " + worker + ": " + e.getMessage());
        }
    }
}
