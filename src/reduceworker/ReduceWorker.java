package reduceworker;

import common.Message;
import common.MessageType;
import common.Protocol;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

public class ReduceWorker {
    private static final Map<String, Integer> globalMap = new HashMap<>();
    private static volatile boolean running = true;
    private static int workerId;

    public static void main(String[] args) {
        System.out.println("ReduceWorker started");

        String workerIdStr = System.getenv("WORKER_ID");
        if (workerIdStr == null) {
            System.err.println("Error: WORKER_ID environment variable not set.");
            return;
        }
        workerId = Integer.parseInt(workerIdStr);

        try (ServerSocket serverSocket = new ServerSocket(Protocol.REDUCE_WORKER_PORT)) {
            System.out.println("ReduceWorker " + workerId + " listening on port " + Protocol.REDUCE_WORKER_PORT);
            
            while (running) {
                Socket clientSocket = serverSocket.accept();
                new Thread(() -> handleClient(clientSocket)).start();
            }
        } catch (IOException e) {
            if (running) {
                e.printStackTrace();
            }
        }
        System.out.println("ReduceWorker " + workerId + " stopped.");
    }

    private static void handleClient(Socket clientSocket) {
        try (
            ObjectOutputStream out = new ObjectOutputStream(clientSocket.getOutputStream());
            ObjectInputStream in = new ObjectInputStream(clientSocket.getInputStream())
        ) {
            Message request = (Message) in.readObject();
            System.out.println("ReduceWorker " + workerId + " received: " + request.getType());

            switch (request.getType()) {
                case REDUCE_START:
                    String[] mapWorkers = request.getData().split(",");
                    for (String mw : mapWorkers) {
                        String[] parts = mw.split(":");
                        String host = parts[0];
                        int port = Integer.parseInt(parts[1]);
                        fetchFromMapWorker(host, port);
                    }
                    out.writeObject(new Message(MessageType.REDUCE_SUCCESS, globalMap));
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

    private static void fetchFromMapWorker(String host, int port) {
        System.out.println("ReduceWorker " + workerId + " fetching from " + host + ":" + port);
        try (Socket mapSocket = new Socket(host, port);
             ObjectOutputStream out = new ObjectOutputStream(mapSocket.getOutputStream());
             ObjectInputStream in = new ObjectInputStream(mapSocket.getInputStream())) {
            
            out.writeObject(new Message(MessageType.REDUCE_FETCH, String.valueOf(workerId)));
            Message reply = (Message) in.readObject();
            
            if (reply.getType() == MessageType.MAP_DATA) {
                Map<String, Integer> subMap = reply.getWordCounts();
                for (Map.Entry<String, Integer> entry : subMap.entrySet()) {
                    globalMap.merge(entry.getKey(), entry.getValue(), Integer::sum);
                }
                System.out.println("Fetched " + subMap.size() + " words from " + host);
            } else {
                System.err.println("Expected MAP_DATA, got " + reply.getType());
            }
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
