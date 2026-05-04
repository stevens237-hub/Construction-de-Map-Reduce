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

    private static class MapStat {
        final String worker;
        final String split;
        final long processingTimeMs;
        final long totalWords;

        MapStat(String worker, String split, long processingTimeMs, long totalWords) {
            this.worker = worker;
            this.split = split;
            this.processingTimeMs = processingTimeMs;
            this.totalWords = totalWords;
        }
    }

    private static class ReduceStat {
        final String worker;
        final long processingTimeMs;

        ReduceStat(String worker, long processingTimeMs) {
            this.worker = worker;
            this.processingTimeMs = processingTimeMs;
        }
    }

    public static void main(String[] args) {
        System.out.println("Coordinator started");

        List<String> discoveredMapWorkers = new ArrayList<>();
        List<String> discoveredReduceWorkers = new ArrayList<>();
        
        // Mapping pour l'affichage des stats
        Map<String, String> workerNames = new HashMap<>();

        String textsDir  = System.getenv("TEXTS_DIR");
        String outputDir = System.getenv("OUTPUT_DIR");

        if (textsDir == null || outputDir == null) {
            System.err.println("Missing env vars: TEXTS_DIR, OUTPUT_DIR");
            return;
        }

        // Phase 0 — attendre que tous les workers soient prêts
        System.out.println("Waiting for workers to be ready...");
        int totalExpected = 0;
        try {
            totalExpected = Integer.parseInt(System.getenv("NB_MAPS")) + Integer.parseInt(System.getenv("NB_REDUCES"));
        } catch (Exception e) {
            System.err.println("NB_MAPS or NB_REDUCES not set or invalid. Defaulting to 4 workers total.");
            totalExpected = 4;
        }

        if (!waitForWorkers(totalExpected, discoveredMapWorkers, discoveredReduceWorkers, workerNames)) {
            System.err.println("Timeout waiting for workers. Proceeding with " + (discoveredMapWorkers.size() + discoveredReduceWorkers.size()) + " workers.");
        }
        System.out.println("Discovered " + discoveredMapWorkers.size() + " MapWorkers and " + discoveredReduceWorkers.size() + " ReduceWorkers.");

        if (discoveredMapWorkers.isEmpty() || discoveredReduceWorkers.isEmpty()) {
            System.err.println("Need at least 1 MapWorker and 1 ReduceWorker. Aborting.");
            return;
        }

        String[] mapWorkers = discoveredMapWorkers.toArray(new String[0]);
        String[] reduceWorkers = discoveredReduceWorkers.toArray(new String[0]);

        long totalStart = System.currentTimeMillis();

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

        long totalDataBytes = splits.stream().mapToLong(s -> s.length).sum();
        System.out.println("Total splits: " + splits.size() + " sur " + mapWorkers.length + " mappers");

        List<MapStat> mapStats = Collections.synchronizedList(new ArrayList<>());
        long mapStart = System.currentTimeMillis();
        boolean mapOk = runMapPhase(splits, mapWorkers, discoveredReduceWorkers.size(), mapStats);
        long mapDuration = System.currentTimeMillis() - mapStart;

        if (!mapOk) {
            System.err.println("Map phase completed with errors — results may be incomplete.");
        }

        // Phase 2 — phase reduce
        System.out.println("--- Reduce Phase ---");
        List<ReduceStat> reduceStats = Collections.synchronizedList(new ArrayList<>());
        long reduceStart = System.currentTimeMillis();
        
        // On construit la liste des mappers pour les reducers
        String mapWorkersStr = String.join(",", mapWorkers);
        List<Map<String, Integer>> partialResults = runReducePhase(reduceWorkers, mapWorkersStr, reduceStats);
        long reduceDuration = System.currentTimeMillis() - reduceStart;

        // Phase 3 — agrégation finale
        System.out.println("--- Aggregation ---");
        Map<String, Integer> finalMap = new HashMap<>();
        for (Map<String, Integer> part : partialResults) {
            for (Map.Entry<String, Integer> e : part.entrySet()) {
                finalMap.merge(e.getKey(), e.getValue(), Integer::sum);
            }
        }

        long totalDuration = System.currentTimeMillis() - totalStart;

        System.out.println("--- Top 20 Words ---");
        finalMap.entrySet().stream()
                .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                .limit(20)
                .forEach(e -> System.out.println(e.getKey() + ": " + e.getValue()));

        writeOutput(finalMap, outputDir);
        printStatistics(mapStats, reduceStats, finalMap, splits, mapWorkers.length,
                reduceWorkers.length, mapDuration, reduceDuration, totalDuration, totalDataBytes, workerNames);

        // Shutdown
        System.out.println("--- Shutting down workers ---");
        for (String w : mapWorkers)    sendShutdown(w);
        for (String w : reduceWorkers) sendShutdown(w);

        System.out.println("Coordinator finished.");
    }

    // -------------------------------------------------------------------------
    // Phase 0 : attendre les READY
    // -------------------------------------------------------------------------

    private static boolean waitForWorkers(int total, List<String> maps, List<String> reduces, Map<String, String> workerNames) {
        CountDownLatch latch = new CountDownLatch(total);
        try (ServerSocket ss = new ServerSocket(Protocol.COORDINATOR_READY_PORT)) {
            ss.setSoTimeout(5_000);
            long deadline = System.currentTimeMillis() + 60_000;

            while (latch.getCount() > 0 && System.currentTimeMillis() < deadline) {
                try {
                    Socket client = ss.accept();
                    new Thread(() -> {
                        try (client;
                             ObjectInputStream in = new ObjectInputStream(client.getInputStream())) {
                            Message msg = (Message) in.readObject();
                            if (msg.getType() == MessageType.READY) {
                                String rawData = msg.getData();
                                System.out.println("  READY: " + rawData);
                                synchronized (maps) {
                                    if (rawData.startsWith("MAP:")) {
                                        String workerHost = rawData.substring(4);
                                        String fullAddr = workerHost + ":" + Protocol.MAP_WORKER_PORT;
                                        if (!maps.contains(fullAddr)) {
                                            maps.add(fullAddr);
                                            workerNames.put(fullAddr, "Mapper-" + maps.size());
                                            latch.countDown();
                                        }
                                    } else if (rawData.startsWith("REDUCE:")) {
                                        String workerHost = rawData.substring(7);
                                        String fullAddr = workerHost + ":" + Protocol.REDUCE_WORKER_PORT;
                                        if (!reduces.contains(fullAddr)) {
                                            reduces.add(fullAddr);
                                            workerNames.put(fullAddr, "Reducer-" + reduces.size());
                                            latch.countDown();
                                        }
                                    }
                                }
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
            long blockSize = (size + nbMappers - 1) / nbMappers;
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

    private static boolean runMapPhase(List<FileSplit> splits, String[] mapWorkers, int nbReduces, List<MapStat> stats) {
        AtomicInteger failures = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(splits.size());

        for (int i = 0; i < splits.size(); i++) {
            final FileSplit split = splits.get(i);
            final int idx = i;

            new Thread(() -> {
                MapStat stat = null;
                for (int retry = 0; retry < Protocol.MAX_RETRIES && stat == null; retry++) {
                    String worker = mapWorkers[(idx + retry) % mapWorkers.length];
                    stat = sendMapTask(worker, split, nbReduces);
                    if (stat == null && retry < Protocol.MAX_RETRIES - 1) {
                        System.err.println("Retry " + (retry + 1) + " for " + split);
                    }
                }
                if (stat == null) {
                    System.err.println("Failed after " + Protocol.MAX_RETRIES + " retries: " + split);
                    failures.incrementAndGet();
                } else {
                    stats.add(stat);
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

    private static MapStat sendMapTask(String worker, FileSplit split, int nbReduces) {
        String[] parts = worker.split(":");
        try (Socket socket = new Socket(parts[0], Integer.parseInt(parts[1]));
             ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
             ObjectInputStream in   = new ObjectInputStream(socket.getInputStream())) {

            socket.setSoTimeout(Protocol.TIMEOUT_MS);
            out.writeObject(new Message(MessageType.MAP_START, split.path, split.offset, split.length, nbReduces));
            out.flush();

            Message response = (Message) in.readObject();
            if (response.getType() == MessageType.MAP_SUCCESS) {
                System.out.println("Map OK: " + split + " on " + worker
                        + " (" + response.getProcessingTimeMs() + " ms, "
                        + response.getTotalWords() + " mots)");
                return new MapStat(worker, split.toString(),
                        response.getProcessingTimeMs(), response.getTotalWords());
            }
        } catch (Exception e) {
            System.err.println("Map error [" + worker + "]: " + e.getMessage());
        }
        return null;
    }

    // -------------------------------------------------------------------------
    // Phase 2 : exécution des tâches reduce
    // -------------------------------------------------------------------------

    private static List<Map<String, Integer>> runReducePhase(String[] reduceWorkers, String mappersStr, List<ReduceStat> stats) {
        List<Map<String, Integer>> results = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch latch = new CountDownLatch(reduceWorkers.length);

        for (int i = 0; i < reduceWorkers.length; i++) {
            final String worker = reduceWorkers[i];
            final int id = i;
            new Thread(() -> {
                try (Socket socket = new Socket(worker.split(":")[0], Integer.parseInt(worker.split(":")[1]));
                     ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                     ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {
                    
                    socket.setSoTimeout(Protocol.TIMEOUT_MS * 10);
                    // On envoie "id:X;mappers:map1,map2..."
                    String data = "id:" + id + ";mappers:" + mappersStr;
                    out.writeObject(new Message(MessageType.REDUCE_START, data));
                    out.flush();

                    Message resp = (Message) in.readObject();
                    if (resp.getType() == MessageType.REDUCE_SUCCESS) {
                        results.add(resp.getWordCounts());
                        stats.add(new ReduceStat(worker, resp.getProcessingTimeMs()));
                        System.out.println("Reduce OK on " + worker
                                + " (" + resp.getProcessingTimeMs() + " ms)");
                    }
                } catch (Exception e) {
                    System.err.println("Error on reduce worker " + worker + ": " + e.getMessage());
                }
                latch.countDown();
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
    // Affichage des statistiques
    // -------------------------------------------------------------------------

    private static void printStatistics(List<MapStat> mapStats, List<ReduceStat> reduceStats,
                                        Map<String, Integer> finalMap, List<FileSplit> splits,
                                        int nbMappers, int nbReducers,
                                        long mapDuration, long reduceDuration, long totalDuration,
                                        long totalDataBytes, Map<String, String> workerNames) {

        long totalWords  = mapStats.stream().mapToLong(s -> s.totalWords).sum();
        long uniqueWords = finalMap.size();
        double richesse  = totalWords > 0 ? (uniqueWords * 100.0 / totalWords) : 0;
        double debitMBs  = totalDuration > 0 ? (totalDataBytes / 1_048_576.0) / (totalDuration / 1000.0) : 0;

        // Déséquilibre de charge entre mappers (basé sur le nb de mots traités)
        Map<String, Long> wordsByMapper = new LinkedHashMap<>();
        for (MapStat s : mapStats) {
            wordsByMapper.merge(s.worker, s.totalWords, Long::sum);
        }
        double imbalance = 0;
        if (wordsByMapper.size() > 1) {
            long max = wordsByMapper.values().stream().mapToLong(Long::longValue).max().orElse(0);
            long min = wordsByMapper.values().stream().mapToLong(Long::longValue).min().orElse(0);
            long avg = (long) wordsByMapper.values().stream().mapToLong(Long::longValue).average().orElse(1);
            imbalance = avg > 0 ? (max - min) * 100.0 / avg : 0;
        }

        String sep   = "╠══════════════════════════════════════════════════╣";
        String top   = "╔══════════════════════════════════════════════════╗";
        String bot   = "╚══════════════════════════════════════════════════╝";
        int W = 50;

        System.out.println(top);
        System.out.println(row("       STATISTIQUES MAPREDUCE", W));
        System.out.println(sep);
        long nbFiles = splits.stream().map(s -> s.path).distinct().count();
        System.out.println(row("  Fichiers : " + nbFiles + "   Splits : " + splits.size()
                + "   Mappers : " + nbMappers + "   Reducers : " + nbReducers, W));
        System.out.println(row("  Total mots    : " + String.format("%,d", totalWords), W));
        System.out.println(row("  Mots uniques  : " + String.format("%,d", uniqueWords)
                + "  (richesse : " + String.format("%.1f", richesse) + "%)", W));
        System.out.println(sep);
        System.out.println(row("  PHASE MAP                       " + mapDuration + " ms", W));
        for (MapStat s : mapStats) {
            String displayName = workerNames.getOrDefault(s.worker, s.worker);
            System.out.println(row("    " + displayName + "  :  " + s.processingTimeMs
                    + " ms   " + String.format("%,d", s.totalWords) + " mots", W));
        }
        if (wordsByMapper.size() > 1) {
            System.out.println(row("  Déséquilibre MAP  :  " + String.format("%.1f", imbalance) + "%", W));
        }
        System.out.println(sep);
        System.out.println(row("  PHASE REDUCE                    " + reduceDuration + " ms", W));
        for (ReduceStat s : reduceStats) {
            String displayName = workerNames.getOrDefault(s.worker, s.worker);
            System.out.println(row("    " + displayName + "  :  " + s.processingTimeMs + " ms", W));
        }
        System.out.println(sep);
        System.out.println(row("  TOTAL                           " + totalDuration + " ms", W));
        System.out.println(row("  Débit  :  " + String.format("%.2f", debitMBs) + " MB/s", W));
        System.out.println(bot);
    }

    private static String row(String content, int width) {
        int innerWidth = width - 2;
        if (content.length() > innerWidth) content = content.substring(0, innerWidth);
        return "║" + content + " ".repeat(innerWidth - content.length()) + "║";
    }

    // -------------------------------------------------------------------------
    // Écriture du fichier de sortie
    // -------------------------------------------------------------------------

    private static void writeOutput(Map<String, Integer> result, String outputDir) {
        Path outputPath = Paths.get(outputDir, "output.txt");
        try { Files.createDirectories(outputPath.getParent()); } catch (IOException ignored) {}
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
