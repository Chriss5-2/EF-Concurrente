package com.tuproyecto.protocol;

import java.util.*;
import java.util.stream.Collectors;

public class ProtocolParser {

    public static Map<String, String> parse(String message) {
        Map<String, String> map = new HashMap<>();
        if (message == null || message.trim().isEmpty()) return map;
        String[] pairs = message.split(";");
        for (String pair : pairs) {
            String[] kv = pair.split("=", 2);
            if (kv.length == 2) {
                map.put(kv[0], kv[1]);
            }
        }
        return map;
    }

    public static String create(Map<String, String> map) {
        return map.entrySet().stream()
                .map(entry -> entry.getKey() + "=" + entry.getValue())
                .collect(Collectors.joining(";"));
    }

    public static List<Double> parseData(String dataStr) {
        if (dataStr == null || dataStr.isEmpty()) {
            return new ArrayList<>();
        }
        return Arrays.stream(dataStr.split(","))
                     .map(Double::parseDouble)
                     .collect(Collectors.toList());
    }
}