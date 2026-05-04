package coordinator;

import common.Message;
import common.MessageType;

import java.io.*;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

public class Coordinator {
    public static void main(String[] args) {
        System.out.println("Coordinator started");
        
        String mapWorkersEnv = System.getenv("MAP_WORKERS");
        String reduceWorkersEnv = System.getenv("REDUCE_WORKERS");
        String textsDir = System.getenv("TEXTS_DIR");

        if (mapWorkersEnv == null || reduceWorkersEnv == null || textsDir == null) {
            System.err.println("Environment variables MAP_WORKERS, REDUCE_WORKERS, TEXTS_DIR must be set.");
            return;
        }

        String[] mapWorkers = mapWorkersEnv.split(",");
        String[] reduceWorkers = reduceWorkersEnv.split(",");

        // Give workers some time to start their ServerSockets
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // Map Phase
        System.out.println("--- Starting Map Phase ---");
        List<Path> files;
        try {
            files = Files.list(Paths.get(textsDir))
                    .filter(Files::isRegularFile)
                    .filter(p -> p.toString().endsWith(".txt"))
                    .collect(Collectors.toList());
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }

        if (files.isEmpty()) {
            System.out.println("No text files found in " + textsDir);
            return;
        }

        CountDownLatch mapLatch = new CountDownLatch(files.size());
        
        for (int i = 0; i < files.size(); i++) {
            String mapWorker = mapWorkers[i % mapWorkers.length];
            String filePath = files.get(i).toString();
            
            new Thread(() -> {
                String[] parts = mapWorker.split(":");
                try (Socket socket = new Socket(parts[0], Integer.parseInt(parts[1]));
                     ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                     ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {
                    
                    out.writeObject(new Message(MessageType.MAP_START, filePath));
                    Message response = (Message) in.readObject();
                    if (response.getType() == MessageType.MAP_SUCCESS) {
                        System.out.println("Mapped " + filePath + " successfully on " + mapWorker);
                    }
                } catch (Exception e) {
                    System.err.println("Error mapping " + filePath + " on " + mapWorker);
                    e.printStackTrace();
                } finally {
                    mapLatch.countDown();
                }
            }).start();
        }

        try {
            mapLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // Reduce Phase
        System.out.println("--- Starting Reduce Phase ---");
        CountDownLatch reduceLatch = new CountDownLatch(reduceWorkers.length);
        List<Map<String, Integer>> allResults = Collections.synchronizedList(new ArrayList<>());

        for (String reduceWorker : reduceWorkers) {
            new Thread(() -> {
                String[] parts = reduceWorker.split(":");
                try (Socket socket = new Socket(parts[0], Integer.parseInt(parts[1]));
                     ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                     ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {
                    
                    out.writeObject(new Message(MessageType.REDUCE_START, mapWorkersEnv));
                    Message response = (Message) in.readObject();
                    
                    if (response.getType() == MessageType.REDUCE_SUCCESS) {
                        allResults.add(response.getWordCounts());
                        System.out.println("Reduced successfully on " + reduceWorker);
                    }
                } catch (Exception e) {
                    System.err.println("Error reducing on " + reduceWorker);
                    e.printStackTrace();
                } finally {
                    reduceLatch.countDown();
                }
            }).start();
        }

        try {
            reduceLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // Final Aggregation
        System.out.println("--- Final Aggregation ---");
        Map<String, Integer> finalMap = new HashMap<>();
        for (Map<String, Integer> subMap : allResults) {
            for (Map.Entry<String, Integer> entry : subMap.entrySet()) {
                finalMap.merge(entry.getKey(), entry.getValue(), Integer::sum);
            }
        }

        System.out.println("Total unique words: " + finalMap.size());
        System.out.println("--- Top 20 Words ---");
        finalMap.entrySet().stream()
                .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                .limit(20)
                .forEach(e -> System.out.println(e.getKey() + ": " + e.getValue()));

        // Shutdown
        System.out.println("--- Shutting down workers ---");
        for (String worker : mapWorkers) {
            sendShutdown(worker);
        }
        for (String worker : reduceWorkers) {
            sendShutdown(worker);
        }
        
        System.out.println("Coordinator finished.");
    }

    private static void sendShutdown(String worker) {
        String[] parts = worker.split(":");
        try (Socket socket = new Socket(parts[0], Integer.parseInt(parts[1]));
             ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream())) {
            out.writeObject(new Message(MessageType.SHUTDOWN));
        } catch (Exception e) {
            System.err.println("Could not send SHUTDOWN to " + worker);
        }
    }
}
