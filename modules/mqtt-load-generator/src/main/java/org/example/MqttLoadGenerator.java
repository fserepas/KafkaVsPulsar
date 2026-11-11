package org.example;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MqttLoadGenerator {
    public static void main(String[] args) {
        ProducerConfig config;
        try {
            config = ProducerConfig.fromArgs(args);
        } catch (IllegalArgumentException ex) {
            System.err.println(ex.getMessage());
            return;
        }

        String payload = PayloadBuilder.build(config.payloadSize());
        ExecutorService executorService = Executors.newFixedThreadPool(config.producerCount());

        for (int i = 0; i < config.producerCount(); i++) {
            final int producerId = i + 1;
            final String clientId = "JavaProducer-" + UUID.randomUUID();
            executorService.submit(new ProducerWorker(clientId, producerId, payload, config));
        }

        executorService.shutdown();
    }
}
