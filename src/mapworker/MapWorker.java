package mapworker;

import common.Message;
import common.MessageType;
import common.Protocol;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MapWorker {
    private static final Map<String, Integer> currentCounts = new ConcurrentHashMap<>();
    private static volatile boolean running = true;
    private static int nbReduces;

    public static void main(String[] args) {
        System.out.println("MapWorker started");
        
        String nbReducesStr = System.getenv("NB_REDUCES");
        if (nbReducesStr == null) {
            System.err.println("Error: NB_REDUCES environment variable not set.");
            return;
        }
        nbReduces = Integer.parseInt(nbReducesStr);

        try (ServerSocket serverSocket = new ServerSocket(Protocol.MAP_WORKER_PORT)) {
            System.out.println("MapWorker listening on port " + Protocol.MAP_WORKER_PORT);
            
            while (running) {
                Socket clientSocket = serverSocket.accept();
                new Thread(() -> handleClient(clientSocket)).start();
            }
        } catch (IOException e) {
            if (running) {
                e.printStackTrace();
            }
        }
        System.out.println("MapWorker stopped.");
    }

    private static void handleClient(Socket clientSocket) {
        try (
            ObjectOutputStream out = new ObjectOutputStream(clientSocket.getOutputStream());
            ObjectInputStream in = new ObjectInputStream(clientSocket.getInputStream())
        ) {
            Message request = (Message) in.readObject();
            System.out.println("MapWorker received: " + request.getType());

            switch (request.getType()) {
                case MAP_START:
                    String filePath = request.getData();
                    processFile(filePath);
                    out.writeObject(new Message(MessageType.MAP_SUCCESS));
                    break;
                case REDUCE_FETCH:
                    int workerId = Integer.parseInt(request.getData());
                    Map<String, Integer> subMap = new HashMap<>();
                    for (Map.Entry<String, Integer> entry : currentCounts.entrySet()) {
                        if (Protocol.getReduceWorkerForWord(entry.getKey(), nbReduces) == workerId) {
                            subMap.put(entry.getKey(), entry.getValue());
                        }
                    }
                    out.writeObject(new Message(MessageType.MAP_DATA, subMap));
                    break;
                case SHUTDOWN:
                    running = false;
                    break;
                default:
                    System.out.println("Unknown message type: " + request.getType());
            }
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            try {
                clientSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static void processFile(String filePath) {
        System.out.println("Processing file: " + filePath);
        try {
            String content = new String(Files.readAllBytes(Paths.get(filePath)));
            String[] words = content.split("\\W+");
            for (String w : words) {
                if (w.isEmpty()) continue;
                String word = w.toLowerCase();
                currentCounts.merge(word, 1, Integer::sum);
            }
            System.out.println("Finished processing " + filePath + ". Total unique words so far: " + currentCounts.size());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
