package com.tuproyecto.worker;

import com.tuproyecto.protocol.ProtocolParser;

import java.io.*;
import java.lang.management.ManagementFactory; // Import para monitoreo de memoria
import java.lang.management.MemoryMXBean;      // Import para monitoreo de memoria
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Worker {
    private final String id;
    private final int tcpPort;
    private final InetSocketAddress maestroAddr;
    private final ExecutorService taskExecutor;
    private final Map<String, List<Double>> dataStore = new ConcurrentHashMap<>();
    private final int numCores;
    private final MemoryMXBean memoryBean; // Bean para monitorear la memoria

    public Worker(String host, int tcpPort, String maestroHost, int maestroPort) {
        this.tcpPort = tcpPort;
        this.id = "worker-" + tcpPort;
        this.maestroAddr = new InetSocketAddress(maestroHost, maestroPort);
        this.numCores = Runtime.getRuntime().availableProcessors();
        this.taskExecutor = Executors.newFixedThreadPool(numCores);
        this.memoryBean = ManagementFactory.getMemoryMXBean(); // Inicializamos el bean
    }

    public void start() {
        System.out.printf("[%s] Iniciando con %d núcleos lógicos...\n", id, numCores);
        new Thread(this::sendHeartbeats, "Worker-Heartbeat-" + id).start();
        new Thread(this::listenForTasks, "Worker-TaskListener-" + id).start();
    }
    
    private void sendHeartbeats() {
        Map<String, String> registerMsgMap = new HashMap<>();
        registerMsgMap.put("TYPE", "REGISTER_WORKER");
        registerMsgMap.put("WORKER_ID", id);
        registerMsgMap.put("TCP_PORT", String.valueOf(tcpPort));
        sendUdpMessage(ProtocolParser.create(registerMsgMap));
        System.out.printf("[%s] Mensaje de registro enviado al Maestro.\n", id);

        Map<String, String> heartbeatMsgMap = new HashMap<>();
        heartbeatMsgMap.put("TYPE", "HEARTBEAT");
        heartbeatMsgMap.put("WORKER_ID", id);
        String heartbeatMsg = ProtocolParser.create(heartbeatMsgMap);

        try (DatagramSocket socket = new DatagramSocket()) {
            while (!Thread.currentThread().isInterrupted()) {
                byte[] buffer = heartbeatMsg.getBytes();
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length, maestroAddr);
                socket.send(packet);
                Thread.sleep(3000);
            }
        } catch (Exception e) {
            System.err.printf("[%s] Error en el hilo de heartbeats: %s\n", id, e.getMessage());
        }
    }

    private void listenForTasks() {
        try (ServerSocket serverSocket = new ServerSocket(tcpPort)) {
            System.out.printf("[%s] Escuchando tareas en TCP:%d\n", id, tcpPort);
            while (true) {
                Socket clientSocket = serverSocket.accept();
                taskExecutor.submit(() -> handleTask(clientSocket));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void handleTask(Socket clientSocket) {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
             PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true)) {

            String line = in.readLine();
            if (line == null) return;

            Map<String, String> message = ProtocolParser.parse(line);
            String type = message.get("TYPE");
            String chunkId = message.get("CHUNK_ID");

            if ("DISTRIBUTE_TASK".equals(type)) {
                String role = message.get("ROLE");
                List<Double> data = ProtocolParser.parseData(message.get("DATA"));
                
                System.out.printf("[%s] Recibido '%s' (tamaño: %d), rol: %s\n", id, chunkId, data.size(), role);
                dataStore.put(chunkId, data);
                
                // --- INICIO DE LA MEJORA DE MONITOREO DE MEMORIA ---
                long heapMemoryUsed = memoryBean.getHeapMemoryUsage().getUsed();
                System.out.printf("[%s] [MONITOR] Uso de memoria (Heap) después de recibir datos: %.2f MB\n", id, (double) heapMemoryUsed / (1024 * 1024));
                // --- FIN DE LA MEJORA DE MONITOREO DE MEMORIA ---

                if ("PRIMARY".equals(role)) {
                    List<Double> resultData = processChunkLocally(data, message.get("OPERATION"));
                    sendResult(out, message.get("JOB_ID"), chunkId, resultData);
                }
            } else if ("PROMOTE_AND_EXECUTE".equals(type)) {
                 System.out.printf("[%s] [RECOVERY] Promovido a PRIMARIO para '%s'. Ejecutando...\n", id, chunkId);
                 List<Double> data = dataStore.get(chunkId);
                 if (data != null) {
                    List<Double> resultData = processChunkLocally(data, message.get("OPERATION"));
                    sendResult(out, message.get("JOB_ID"), chunkId, resultData);
                 } else {
                     System.err.printf("[%s] [ERROR] No se encontraron datos para el chunk promovido %s\n", id, chunkId);
                 }
            }
        } catch (IOException e) {
            // Silencioso para desconexiones normales, ej. cuando una réplica no necesita responder.
        } finally {
            try { clientSocket.close(); } catch (IOException e) { /* ignorar */ }
        }
    }
    
    private void sendResult(PrintWriter out, String jobId, String chunkId, List<Double> resultData) {
        Map<String, String> responseMap = new HashMap<>();
        responseMap.put("TYPE", "TASK_RESULT");
        responseMap.put("JOB_ID", jobId);
        responseMap.put("CHUNK_ID", chunkId);
        responseMap.put("STATUS", "SUCCESS");
        responseMap.put("DATA", resultData.stream().map(String::valueOf).collect(Collectors.joining(",")));
        out.println(ProtocolParser.create(responseMap));
    }

    private List<Double> processChunkLocally(List<Double> data, String operationId) {
        final Function<Double, Double> operation;

        if ("COMPLEX_OP".equals(operationId)) {
            operation = (x) -> (Math.pow(Math.sin(x) + Math.cos(x), 2)) / (Math.sqrt(Math.abs(x)) + 1);
        } else if ("CONDITIONAL_OP_INT".equals(operationId)) {
            operation = (x) -> (x % 3 == 0 || (x >= 500 && x <= 1000)) ? (x * Math.log(x)) % 7 : x;
        } else {
            operation = (x) -> x; // Operación por defecto
        }

        // Envoltura de la operación para Resiliencia Local
        Function<Double, Double> resilientOperation = (x) -> {
            try {
                return operation.apply(x);
            } catch (Exception e) {
                System.err.printf("[%s] [RESILIENCIA LOCAL] Error procesando valor %.2f: %s. Devolviendo -1.0\n", id, x, e.getMessage());
                return -1.0; // Valor de error estándar
            }
        };

        System.out.printf("[%s] Procesando chunk de tamaño %d con op '%s' en paralelo...\n", id, data.size(), operationId);
        return data.parallelStream().map(resilientOperation).collect(Collectors.toList());
    }

    private void sendUdpMessage(String message) {
        try (DatagramSocket socket = new DatagramSocket()) {
            byte[] buffer = message.getBytes();
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length, maestroAddr);
            socket.send(packet);
        } catch (IOException e) {
            System.err.printf("[%s] Error enviando UDP: %s\n", id, e.getMessage());
        }
    }
}