package reduceworker;

import common.Message;
import common.MessageType;
import common.Protocol;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

public class ReduceWorker {

    private static final Map<String, Integer> globalMap = new ConcurrentHashMap<>();
    private static volatile boolean running = true;
    private static int workerId;

    public static void main(String[] args) {
        String workerIdStr     = System.getenv("WORKER_ID");
        String coordinatorHost = System.getenv("COORDINATOR_HOST");
        String hostname        = System.getenv("HOSTNAME");

        if (workerIdStr == null || coordinatorHost == null) {
            System.err.println("Missing env vars: WORKER_ID, COORDINATOR_HOST");
            return;
        }
        workerId = Integer.parseInt(workerIdStr);
        String workerName = hostname != null ? hostname : "reduce-worker-" + workerId;

        try (ServerSocket serverSocket = new ServerSocket(Protocol.REDUCE_WORKER_PORT)) {
            System.out.println("ReduceWorker " + workerId + " listening on port " + Protocol.REDUCE_WORKER_PORT);

            notifyReady(coordinatorHost, workerName);

            while (running) {
                Socket clientSocket = serverSocket.accept();
                new Thread(() -> handleClient(clientSocket)).start();
            }
        } catch (IOException e) {
            if (running) e.printStackTrace();
        }
        System.out.println("ReduceWorker " + workerId + " stopped.");
    }

    // -------------------------------------------------------------------------
    // Signal de disponibilité au coordinator
    // -------------------------------------------------------------------------

    private static void notifyReady(String coordinatorHost, String workerName) {
        for (int attempt = 0; attempt < 20; attempt++) {
            try {
                if (attempt > 0) Thread.sleep(1000);
                try (Socket s = new Socket(coordinatorHost, Protocol.COORDINATOR_READY_PORT);
                     ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream())) {
                    s.setSoTimeout(Protocol.TIMEOUT_MS);
                    out.writeObject(new Message(MessageType.READY, workerName));
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
            // Timeout généreux : le reducer doit contacter tous les mappers
            clientSocket.setSoTimeout(Protocol.TIMEOUT_MS * 20);
            ObjectOutputStream out = new ObjectOutputStream(clientSocket.getOutputStream());
            ObjectInputStream  in  = new ObjectInputStream(clientSocket.getInputStream());

            Message request = (Message) in.readObject();
            System.out.println("ReduceWorker " + workerId + " received: " + request.getType());

            switch (request.getType()) {
                case REDUCE_START:
                    String[] mapWorkers = request.getData().split(",");
                    fetchFromAllMapWorkers(mapWorkers);
                    out.writeObject(new Message(MessageType.REDUCE_SUCCESS, globalMap));
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
    // Fetch parallèle depuis tous les mappers
    // -------------------------------------------------------------------------

    private static void fetchFromAllMapWorkers(String[] mapWorkers) {
        CountDownLatch latch = new CountDownLatch(mapWorkers.length);

        for (String mw : mapWorkers) {
            String[] parts = mw.split(":");
            String host = parts[0];
            int port    = Integer.parseInt(parts[1]);

            new Thread(() -> {
                try {
                    fetchFromMapWorker(host, port);
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
    }

    private static void fetchFromMapWorker(String host, int port) {
        System.out.println("ReduceWorker " + workerId + " fetching from " + host + ":" + port);
        try (Socket mapSocket = new Socket(host, port);
             ObjectOutputStream out = new ObjectOutputStream(mapSocket.getOutputStream());
             ObjectInputStream  in  = new ObjectInputStream(mapSocket.getInputStream())) {

            mapSocket.setSoTimeout(Protocol.TIMEOUT_MS);
            out.writeObject(new Message(MessageType.REDUCE_FETCH, String.valueOf(workerId)));
            out.flush();

            Message reply = (Message) in.readObject();
            if (reply.getType() == MessageType.MAP_DATA) {
                Map<String, Integer> subMap = reply.getWordCounts();
                // ConcurrentHashMap.merge est thread-safe
                for (Map.Entry<String, Integer> entry : subMap.entrySet()) {
                    globalMap.merge(entry.getKey(), entry.getValue(), Integer::sum);
                }
                System.out.println("Fetched " + subMap.size() + " words from " + host);
            } else {
                System.err.println("Expected MAP_DATA from " + host + ", got " + reply.getType());
            }
        } catch (IOException | ClassNotFoundException e) {
            System.err.println("Error fetching from " + host + ": " + e.getMessage());
        }
    }
}
