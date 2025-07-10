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

public class DArrayInt {
    private final String maestroHost;
    private final int maestroTcpPort;
    private List<Integer> localData;

    public DArrayInt(String maestroHost, int maestroTcpPort, List<Integer> data) {
        this.maestroHost = maestroHost;
        this.maestroTcpPort = maestroTcpPort;
        this.localData = new ArrayList<>(data);
    }

    public DArrayInt map(String operationId) {
        System.out.printf("[DArrayInt] Enviando trabajo al Maestro...\n");
        List<Double> dataAsDouble = this.localData.stream().map(Integer::doubleValue).collect(Collectors.toList());

        try (Socket socket = new Socket(maestroHost, maestroTcpPort);
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
            
            Map<String, String> jobRequestMap = new HashMap<>();
            jobRequestMap.put("TYPE", "CLIENT_JOB");
            jobRequestMap.put("JOB_ID", "job-" + System.currentTimeMillis());
            jobRequestMap.put("OPERATION", operationId);
            String dataStr = dataAsDouble.stream().map(String::valueOf).collect(Collectors.joining(","));
            jobRequestMap.put("DATA", dataStr);
            out.println(ProtocolParser.create(jobRequestMap));

            String responseLine = in.readLine();
            if (responseLine == null) return null;

            Map<String, String> response = ProtocolParser.parse(responseLine);

            if ("JOB_COMPLETE".equals(response.get("TYPE")) && "SUCCESS".equals(response.get("STATUS"))) {
                List<Integer> resultData = ProtocolParser.parseData(response.get("DATA")).stream()
                                                         .map(Double::intValue).collect(Collectors.toList());
                System.out.println("[DArrayInt] ¡Trabajo completado exitosamente!");
                return new DArrayInt(maestroHost, maestroTcpPort, resultData);
            } else {
                System.err.println("[DArrayInt] El trabajo falló.");
                return null;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public List<Integer> collect() {
        return new ArrayList<>(this.localData);
    }
    
    @Override
    public String toString() {
        String dataPreview = localData.stream().limit(5).map(String::valueOf).collect(Collectors.joining(", "));
        return "DArrayInt(size=" + localData.size() + ", data=[" + dataPreview + "...])";
    }
}