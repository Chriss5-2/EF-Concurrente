package com.tuproyecto.maestro;

import com.tuproyecto.protocol.ProtocolParser;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class Maestro {
    private final int udpPort;
    private final int tcpPort;

    // --- ESTADO CENTRALIZADO Y CONCURRENTE ---
    private final ConcurrentMap<String, WorkerInfo> workers = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Job> jobs = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ChunkInfo> chunkDistribution = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);

    private static class WorkerInfo {
        String id; String host; int tcpPort; long lastHeartbeat; volatile String status = "ALIVE";
        WorkerInfo(String id, String host, int tcpPort) { this.id = id; this.host = host; this.tcpPort = tcpPort; this.lastHeartbeat = System.currentTimeMillis(); }
    }
    
    private static class Job {
        String id; String operation; ConcurrentMap<String, List<Double>> results = new ConcurrentHashMap<>(); CountDownLatch latch;
        Job(String id, String operation, int expectedChunks) {
            this.id = id; this.operation = operation; this.latch = new CountDownLatch(expectedChunks);
        }
    }
    
    private static class ChunkInfo {
        String primaryWorkerId; String replicaWorkerId;
        ChunkInfo(String p, String r) { this.primaryWorkerId = p; this.replicaWorkerId = r; }
    }

    public Maestro(String host, int udpPort, int tcpPort) {
        this.udpPort = udpPort; this.tcpPort = tcpPort;
    }

    public void start() {
        System.out.printf("[MAESTRO] Iniciando en UDP:%d y TCP:%d\n", udpPort, tcpPort);
        new Thread(this::listenForUdpMessages).start();
        scheduler.scheduleAtFixedRate(this::watchdog, 5, 5, TimeUnit.SECONDS);
        new Thread(this::listenForClientJobs).start();
    }

    private void listenForUdpMessages() {
        try (DatagramSocket socket = new DatagramSocket(udpPort)) {
            byte[] buffer = new byte[1024];
            while (true) {
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                socket.receive(packet);
                String messageStr = new String(packet.getData(), 0, packet.getLength());
                Map<String, String> message = ProtocolParser.parse(messageStr);
                String workerId = message.get("WORKER_ID");
                if (workerId == null) continue;

                if ("REGISTER_WORKER".equals(message.get("TYPE"))) {
                    int workerTcpPort = Integer.parseInt(message.get("TCP_PORT"));
                    String workerHost = packet.getAddress().getHostAddress();
                    workers.put(workerId, new WorkerInfo(workerId, workerHost, workerTcpPort));
                    System.out.printf("[MAESTRO] Worker '%s' registrado.\n", workerId);
                } else if ("HEARTBEAT".equals(message.get("TYPE"))) {
                    WorkerInfo info = workers.get(workerId);
                    if (info != null) {
                        info.lastHeartbeat = System.currentTimeMillis();
                        if ("DEAD".equals(info.status)) {
                            info.status = "ALIVE";
                            System.out.printf("[MAESTRO] Worker '%s' ha revivido.\n", workerId);
                        }
                    }
                }
            }
        } catch (IOException e) { e.printStackTrace(); }
    }

    private void watchdog() {
        long now = System.currentTimeMillis();
        for (WorkerInfo info : workers.values()) {
            if ("ALIVE".equals(info.status) && (now - info.lastHeartbeat) > 8000) {
                info.status = "DEAD";
                System.err.printf("[MAESTRO] [WATCHDOG] Worker '%s' marcado como DEAD.\n", info.id);
                scheduler.submit(() -> handleWorkerFailure(info.id));
            }
        }
    }

    private void handleWorkerFailure(String deadWorkerId) {
        System.out.printf("[MAESTRO] [RECOVERY] Iniciando recuperación para el worker caído %s\n", deadWorkerId);
        for (Map.Entry<String, ChunkInfo> entry : chunkDistribution.entrySet()) {
            String chunkId = entry.getKey();
            ChunkInfo chunkInfo = entry.getValue();

            if (deadWorkerId.equals(chunkInfo.primaryWorkerId)) {
                System.out.printf("[MAESTRO] [RECOVERY] Chunk %s necesita recuperación.\n", chunkId);
                String replicaId = chunkInfo.replicaWorkerId;
                WorkerInfo replicaWorker = workers.get(replicaId);
                
                if (replicaWorker != null && "ALIVE".equals(replicaWorker.status)) {
                    System.out.printf("[MAESTRO] [RECOVERY] Promoviendo a %s para el chunk %s\n", replicaId, chunkId);
                    chunkInfo.primaryWorkerId = replicaId;
                    WorkerInfo newReplica = selectReplicaNode(Arrays.asList(replicaId));
                    chunkInfo.replicaWorkerId = (newReplica != null) ? newReplica.id : null;
                    
                    String jobId = chunkId.split("-c")[0];
                    Job job = jobs.get(jobId);
                    if (job != null) {
                        new Thread(() -> sendPromotionToWorker(replicaWorker, jobId, chunkId, job.operation)).start();
                    }
                } else {
                    System.err.printf("[MAESTRO] [CRITICAL] ¡PÉRDIDA DE DATOS! No se encontró réplica viva para el chunk %s\n", chunkId);
                    String jobId = chunkId.split("-c")[0];
                    Job job = jobs.get(jobId);
                    if (job != null && job.latch.getCount() > 0) job.latch.countDown();
                }
            }
        }
    }

    private void listenForClientJobs() {
        try (ServerSocket serverSocket = new ServerSocket(tcpPort)) {
            while (true) {
                Socket clientSocket = serverSocket.accept();
                new Thread(() -> handleClientJob(clientSocket)).start();
            }
        } catch (IOException e) { e.printStackTrace(); }
    }

    private void handleClientJob(Socket clientSocket) {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
             PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true)) {

            String line = in.readLine();
            Map<String, String> message = ProtocolParser.parse(line);
            List<Double> data = ProtocolParser.parseData(message.get("DATA"));
            String operation = message.get("OPERATION");
            String jobId = message.get("JOB_ID");

            List<WorkerInfo> aliveWorkers = workers.values().stream().filter(w -> "ALIVE".equals(w.status)).collect(Collectors.toList());
            if (aliveWorkers.isEmpty()) {
                out.println("TYPE=JOB_FAILED;REASON=NO_WORKERS_AVAILABLE"); return;
            }

            int n_workers = aliveWorkers.size();
            List<List<Double>> chunks = splitList(data, n_workers);
            Job job = new Job(jobId, operation, chunks.size());
            jobs.put(jobId, job);

            for (int i = 0; i < chunks.size(); i++) {
                final int idx = i;
                String chunkId = jobId + "-c" + idx;
                WorkerInfo primaryWorker = aliveWorkers.get(idx % n_workers);
                WorkerInfo replicaWorker = selectReplicaNode(Arrays.asList(primaryWorker.id));
                chunkDistribution.put(chunkId, new ChunkInfo(primaryWorker.id, replicaWorker != null ? replicaWorker.id : null));
                
                new Thread(() -> sendTaskToWorker(primaryWorker, jobId, chunkId, chunks.get(idx), operation, "PRIMARY")).start();
                if (replicaWorker != null) {
                    System.out.printf("[MAESTRO] Replicando %s en %s\n", chunkId, replicaWorker.id);
                    new Thread(() -> sendTaskToWorker(replicaWorker, jobId, chunkId, chunks.get(idx), operation, "REPLICA")).start();
                }
            }

            boolean finished = job.latch.await(60, TimeUnit.SECONDS);
            
            if(finished) {
                List<Double> finalResult = new ArrayList<>();
                for (int i = 0; i < chunks.size(); i++) {
                    finalResult.addAll(job.results.getOrDefault(jobId + "-c" + i, Collections.emptyList()));
                }
                Map<String, String> responseMap = new HashMap<>();
                responseMap.put("TYPE", "JOB_COMPLETE");
                responseMap.put("STATUS", "SUCCESS");
                responseMap.put("DATA", finalResult.stream().map(String::valueOf).collect(Collectors.joining(",")));
                out.println(ProtocolParser.create(responseMap));
            } else {
                 out.println("TYPE=JOB_FAILED;REASON=TIMEOUT");
            }
        } catch (Exception e) { e.printStackTrace(); }
    }
    
    private void sendTaskToWorker(WorkerInfo worker, String jobId, String chunkId, List<Double> data, String operation, String role) {
        try (Socket socket = new Socket(worker.host, worker.tcpPort);
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
            
            Map<String, String> taskMap = new HashMap<>();
            taskMap.put("TYPE", "DISTRIBUTE_TASK");
            taskMap.put("JOB_ID", jobId);
            taskMap.put("CHUNK_ID", chunkId);
            taskMap.put("ROLE", role);
            taskMap.put("OPERATION", operation);
            taskMap.put("DATA", data.stream().map(String::valueOf).collect(Collectors.joining(",")));
            out.println(ProtocolParser.create(taskMap));

            if ("PRIMARY".equals(role)) {
                String responseLine = in.readLine();
                Map<String, String> response = ProtocolParser.parse(responseLine);
                if("SUCCESS".equals(response.get("STATUS"))) {
                    handleTaskResult(jobId, chunkId, ProtocolParser.parseData(response.get("DATA")));
                }
            }
        } catch (IOException e) {
            System.err.printf("[MAESTRO] No se pudo enviar tarea a %s: %s\n", worker.id, e.getMessage());
        }
    }

    private void sendPromotionToWorker(WorkerInfo worker, String jobId, String chunkId, String operation) {
        try (Socket socket = new Socket(worker.host, worker.tcpPort);
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
             
            Map<String, String> promoMap = new HashMap<>();
            promoMap.put("TYPE", "PROMOTE_AND_EXECUTE");
            promoMap.put("JOB_ID", jobId);
            promoMap.put("CHUNK_ID", chunkId);
            promoMap.put("OPERATION", operation);
            out.println(ProtocolParser.create(promoMap));

            String responseLine = in.readLine();
            Map<String, String> response = ProtocolParser.parse(responseLine);
            if("SUCCESS".equals(response.get("STATUS"))) {
                handleTaskResult(jobId, chunkId, ProtocolParser.parseData(response.get("DATA")));
            }
        } catch (IOException e) {
            System.err.printf("[MAESTRO] No se pudo enviar promoción a %s: %s\n", worker.id, e.getMessage());
        }
    }

    private void handleTaskResult(String jobId, String chunkId, List<Double> resultData) {
        Job job = jobs.get(jobId);
        if (job != null) {
            job.results.put(chunkId, resultData);
            job.latch.countDown();
        }
    }
    
    private WorkerInfo selectReplicaNode(List<String> excludeIds) {
        return workers.values().stream()
                .filter(w -> "ALIVE".equals(w.status) && !excludeIds.contains(w.id))
                .findAny().orElse(null);
    }
    
    private <T> List<List<T>> splitList(List<T> source, int n_parts) {
        List<List<T>> partitions = new ArrayList<>();
        int chunkSize = (int) Math.ceil((double) source.size() / n_parts);
        for (int i = 0; i < source.size(); i += chunkSize) {
            partitions.add(source.subList(i, Math.min(source.size(), i + chunkSize)));
        }
        return partitions;
    }
}