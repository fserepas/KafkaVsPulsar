package org.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.json.KafkaJsonSchemaSerde;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.StreamsConfig;

import java.io.IOException;
import java.time.Duration;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class KafkaStreamsProcessors {
    private static final String INPUT_TOPIC = environment("KAFKA_INPUT_TOPIC", "temp");
    private static final String OUTPUT_TOPIC = environment("KAFKA_OUTPUT_TOPIC", "processed_KafkaStreams_temp");
    private static final String BOOTSTRAP_SERVERS = environment("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092");
    private static final String SCHEMA_REGISTRY_URL = environment("SCHEMA_REGISTRY_URL", "http://schema-registry:8081");

    private static String environment(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }

    public static void main(String[] args) {
        tempProcessor();
    }

    private static void tempProcessor() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, environment("KAFKA_APPLICATION_ID", "temperature_processor"));
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);

        props.put(StreamsConfig.producerPrefix(ProducerConfig.MAX_BLOCK_MS_CONFIG), 10_000);
        props.put(StreamsConfig.producerPrefix(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG), 10_000);
        props.put(StreamsConfig.producerPrefix(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG), 20_000);

        props.put("schema.registry.request.timeout.ms", 5000);
        props.put("schema.registry.http.connect.timeout.ms", 5000);
        props.put("schema.registry.http.read.timeout.ms", 5000);

        StreamsBuilder builder = new StreamsBuilder();
        ObjectMapper objectMapper = new ObjectMapper();
        DateTimeFormatter timestampFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS");

        KafkaJsonSchemaSerde<ProcessedTemp> jsonSchemaSerde = new KafkaJsonSchemaSerde<>();
        jsonSchemaSerde.configure(
                Map.of(
                        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL,
                        AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, true,
                        "json.value.type", ProcessedTemp.class.getName(),
                        "schema.registry.request.timeout.ms", "5000",
                        "schema.registry.http.connect.timeout.ms", "5000",
                        "schema.registry.http.read.timeout.ms", "5000"
                ),
                false
        );

        KStream<String, String> inputStream = builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()));
        System.out.println("Listening to topic: " + INPUT_TOPIC);

        KStream<String, ProcessedTemp> processedStream = inputStream
                .map((key, value) -> {
                    try {
                        JsonNode root = objectMapper.readTree(value);
                        JsonNode payload = root.get("payload");

                        ProcessedTemp out = new ProcessedTemp();
                        out.MESSAGE_ID = payload.path("message_id").asText();
                        out.PRODUCER = key;
                        out.PRODUCED_TIMESTAMP = payload.path("timestamp").asText();
                        out.KAFKA_TIMESTAMP = payload.path("kafka_timestamp").asText();
                        out.PROCESSED_TIMESTAMP = ZonedDateTime.now(ZoneOffset.UTC).format(timestampFormatter);
                        out.TEMPERATURE_FAHRENHEIT = payload.path("temperature").asDouble();
                        out.UPDATED_TEMPERATURE_CELSIUS = (out.TEMPERATURE_FAHRENHEIT - 32) * 5.0 / 9.0;
                        out.PAYLOAD = payload.path("payload").asText();

                        return KeyValue.pair(key, out);
                    } catch (IOException | RuntimeException e) {
                        System.err.println("Skipping invalid record for key '" + key + "': " + e.getMessage());
                        return KeyValue.pair(key, null);
                    }
                })
                .filter((k, v) -> v != null);

        processedStream.to(
                OUTPUT_TOPIC,
                Produced.with(Serdes.String(), jsonSchemaSerde)
        );

        @SuppressWarnings("resource")
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.setUncaughtExceptionHandler(e -> {
            System.err.println("Uncaught stream exception: " + e.getMessage());
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
        });

        CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close(Duration.ofSeconds(60));
            latch.countDown();
        }));

        try {
            streams.start();
            System.out.println("Kafka Streams processor started.");
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Kafka Streams processor interrupted", e);
        } finally {
            streams.close(Duration.ofSeconds(10));
        }
    }
}
