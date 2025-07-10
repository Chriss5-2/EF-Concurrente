package com.tuproyecto.darray;

import com.tuproyecto.protocol.ProtocolParser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DArrayDouble {
    private final String maestroHost;
    private final int maestroTcpPort;
    private List<Double> localData;

    public DArrayDouble(String maestroHost, int maestroTcpPort, List<Double> data) {
        this.maestroHost = maestroHost;
        this.maestroTcpPort = maestroTcpPort;
        this.localData = new ArrayList<>(data);
    }

    public DArrayDouble map(String operationId) {
        System.out.printf("[DArray] Enviando trabajo al Maestro en %s:%d...\n", maestroHost, maestroTcpPort);
        try (Socket socket = new Socket(maestroHost, maestroTcpPort);
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
            
            Map<String, String> jobRequestMap = new HashMap<>();
            jobRequestMap.put("TYPE", "CLIENT_JOB");
            jobRequestMap.put("JOB_ID", "job-" + System.currentTimeMillis());
            jobRequestMap.put("OPERATION", operationId);
            String dataStr = this.localData.stream().map(String::valueOf).collect(Collectors.joining(","));
            jobRequestMap.put("DATA", dataStr);
            out.println(ProtocolParser.create(jobRequestMap));

            String responseLine = in.readLine();
            if (responseLine == null) {
                System.err.println("[DArray] Error: El Maestro cerró la conexión sin respuesta.");
                return null;
            }

            Map<String, String> response = ProtocolParser.parse(responseLine);

            if ("JOB_COMPLETE".equals(response.get("TYPE")) && "SUCCESS".equals(response.get("STATUS"))) {
                List<Double> resultData = ProtocolParser.parseData(response.get("DATA"));
                System.out.println("[DArray] ¡Trabajo completado exitosamente!");
                return new DArrayDouble(maestroHost, maestroTcpPort, resultData);
            } else {
                System.err.println("[DArray] El trabajo falló. Razón: " + response.getOrDefault("REASON", "Desconocida"));
                return null;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public List<Double> collect() {
        return new ArrayList<>(this.localData);
    }

    @Override
    public String toString() {
        String dataPreview = localData.stream().limit(5).map(d -> String.format("%.2f", d)).collect(Collectors.joining(", "));
        return "DArrayDouble(size=" + localData.size() + ", data=[" + dataPreview + "...])";
    }
}