package org.example;

import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import com.google.gson.JsonObject;

public class ProducerWorker implements Runnable {
    private static final DateTimeFormatter TIMESTAMP_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS");

    private final String clientId;
    private final int producerId;
    private final String payload;
    private final ProducerConfig config;

    public ProducerWorker(String clientId, int producerId, String payload, ProducerConfig config) {
        this.clientId = clientId;
        this.producerId = producerId;
        this.payload = payload;
        this.config = config;
    }

    @Override
    public void run() {
        String producerTopic = config.topic();
        Random random = new Random();
        MqttClient client = null;

        try {
            TimeUnit.MILLISECONDS.sleep(random.nextInt(1500));

            MqttConnectOptions options = new MqttConnectOptions();
            options.setCleanSession(true);

            client = new MqttClient(config.brokerAddress(), clientId, new MemoryPersistence());
            client.connect(options);

            for (int i = 1; i <= config.messagesPerProducer(); i++) {
                Random randomOffset = new Random();
                Double randomTemperature = Math.round(randomOffset.nextDouble(86,88) * 100.0) / 100.0;
                JsonObject json = new JsonObject();
                json.addProperty("message_id", UUID.randomUUID().toString());
                json.addProperty("timestamp", ZonedDateTime.now(ZoneOffset.UTC).format(TIMESTAMP_FORMATTER));
                json.addProperty("temperature", randomTemperature);
                json.addProperty("payload", payload);

                MqttMessage message = new MqttMessage(json.toString().getBytes(StandardCharsets.UTF_8));
                message.setQos(0);
                client.publish(producerTopic, message);

                if (i < config.messagesPerProducer()) {
                    TimeUnit.MILLISECONDS.sleep(config.delayBetweenMessagesMs());
                }
            }

        } catch (MqttException e) {
            throw new RuntimeException("Producer " + producerId + " failed to publish", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } finally {
            if (client != null) {
                try {
                    if (client.isConnected()) {
                        client.disconnect();
                    }
                    client.close();
                } catch (MqttException ignored) {
                    // Best effort cleanup.
                }
            }
        }
    }
}
