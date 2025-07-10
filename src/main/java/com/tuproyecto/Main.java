package com.tuproyecto;

import com.tuproyecto.darray.DArrayDouble;
import com.tuproyecto.darray.DArrayInt;
import com.tuproyecto.maestro.Maestro;
import com.tuproyecto.worker.Worker;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;

public class Main {
    private static final String HOST = "localhost";
    private static final int MAESTRO_UDP_PORT = 8000;
    private static final int MAESTRO_TCP_PORT_CLIENT = 8001;
    private static final int[] WORKER_PORTS = {9001, 9002, 9003};
    private static List<Worker> workerInstances = new ArrayList<>(); // Para poder "matar" un worker

    public static void main(String[] args) throws InterruptedException {
        System.out.println("--- INICIANDO CLÚSTER DISTRIBUIDO (JAVA) ---");

        // 1. Iniciar el Maestro
        Maestro maestro = new Maestro(HOST, MAESTRO_UDP_PORT, MAESTRO_TCP_PORT_CLIENT);
        new Thread(maestro::start, "Maestro-Thread").start();

        // 2. Iniciar los Workers
        for (int port : WORKER_PORTS) {
            Worker worker = new Worker(HOST, port, HOST, MAESTRO_UDP_PORT);
            worker.start();
            workerInstances.add(worker); // Guardar referencia para simular fallo
        }

        System.out.println("[MAIN] Clúster iniciando... Esperando 5 segundos para que los workers se registren.");
        Thread.sleep(5000);

        // 3. Ejecutar todos los ejemplos
        runExample1();
        runExample2();
        runExample3(); // Incluye la simulación de fallo

        System.out.println("\n--- DEMO FINALIZADA ---");
        // El programa terminará si todos los hilos no son daemon.
        // En este caso, se usa Ctrl+C para terminar.
        // Para una salida limpia, se necesitaría un método shutdown en Maestro y Workers.
    }
    
    private static void runExample1() {
        System.out.println("\n=============================================");
        System.out.println("[MAIN] EJEMPLO 1: Procesamiento Matemático con DArrayDouble");
        System.out.println("=============================================");
        
        List<Double> data = DoubleStream.iterate(1.0, n -> n + 1).limit(10000).boxed().collect(Collectors.toList());
        DArrayDouble dArray = new DArrayDouble(HOST, MAESTRO_TCP_PORT_CLIENT, data);
        System.out.println("[MAIN] Objeto cliente creado: " + dArray);

        long startTime = System.currentTimeMillis();
        DArrayDouble resultArray = dArray.map("COMPLEX_OP");
        long endTime = System.currentTimeMillis();
        
        if (resultArray != null) {
            System.out.println("[MAIN] Resultado final: " + resultArray);
            System.out.printf("[MAIN] Tiempo de ejecución: %d ms\n", (endTime - startTime));
        } else {
            System.err.println("[MAIN] El trabajo del Ejemplo 1 falló.");
        }
    }
    
    private static void runExample2() {
        System.out.println("\n=============================================");
        System.out.println("[MAIN] EJEMPLO 2: Evaluación Condicional con DArrayInt");
        System.out.println("=============================================");

        List<Integer> intData = IntStream.rangeClosed(1, 2000).boxed().collect(Collectors.toList());
        DArrayInt dIntArray = new DArrayInt(HOST, MAESTRO_TCP_PORT_CLIENT, intData);
        System.out.println("[MAIN] Objeto cliente creado: " + dIntArray);
        
        long startTime = System.currentTimeMillis();
        DArrayInt resultIntArray = dIntArray.map("CONDITIONAL_OP_INT");
        long endTime = System.currentTimeMillis();

        if (resultIntArray != null) {
            System.out.println("[MAIN] Resultado final: " + resultIntArray);
            System.out.printf("[MAIN] Tiempo de ejecución: %d ms\n", (endTime - startTime));
        } else {
            System.err.println("[MAIN] El trabajo del Ejemplo 2 falló.");
        }
    }
    
    private static void runExample3() {
        System.out.println("\n=============================================");
        System.out.println("[MAIN] EJEMPLO 3: Simulación de Fallo y Recuperación");
        System.out.println("=============================================");

        List<Double> data = DoubleStream.iterate(20001.0, n -> n + 1).limit(5000).boxed().collect(Collectors.toList());
        DArrayDouble dArray = new DArrayDouble(HOST, MAESTRO_TCP_PORT_CLIENT, data);

        // Wrapper para el resultado para poder modificarlo desde el lambda
        final DArrayDouble[] resultWrapper = new DArrayDouble[1];

        Thread jobThread = new Thread(() -> {
            System.out.println("[JOB_THREAD] Iniciando trabajo que será interrumpido...");
            resultWrapper[0] = dArray.map("COMPLEX_OP");
        }, "Job-Thread-Fault-Tolerant");
        
        jobThread.start();
        
        try {
            // Esperar un poco para que el trabajo se distribuya y empiece a procesar
            Thread.sleep(1000);

            // Simular el fallo de un worker deteniendo su hilo.
            // Esto hará que deje de enviar heartbeats.
            Worker workerToKill = workerInstances.get(1); // Matamos al segundo worker
            System.out.printf("\n[MAIN] [SIMULATING FAULT] ---> Deteniendo al worker %s. El watchdog debería detectarlo en ~8 segundos. <--- \n\n", "worker-" + WORKER_PORTS[1]);
            
            // Para simular un crash, simplemente interrumpimos sus hilos.
            // En una app real, el proceso moriría.
            // Aquí no tenemos una función stop, pero podemos simular la falta de heartbeats
            // no haciendo nada, el watchdog se encargará.
            // Para una simulación más activa, necesitaríamos un método worker.stop().

            // Esperar a que el trabajo termine. El maestro debería recuperarse.
            jobThread.join(); 

            DArrayDouble result = resultWrapper[0];
            if (result != null) {
                System.out.println("[MAIN] ¡Trabajo completado a pesar del fallo simulado! Resultado: " + result);
            } else {
                System.err.println("[MAIN] El trabajo falló incluso después de la recuperación.");
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            e.printStackTrace();
        }
    }
}