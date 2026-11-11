package org.example;

public record ProducerConfig(
        int delayBetweenMessagesMs,
        int messagesPerProducer,
        int producerCount,
        int payloadSize,
        String topic,
        String brokerAddress
) {
    static ProducerConfig fromArgs(String[] args) {
        int delayBetweenMessagesMs = 1000;
        int messagesPerProducer = 600;
        int producerCount = 100;
        int payloadSize = 130;
        String topic = "";
        String brokerAddress = "";

        for (String arg : args) {
            if (arg.startsWith("--delay-between-messages=")) {
                delayBetweenMessagesMs = parseIntArg(arg, "--delay-between-messages=");
            } else if (arg.startsWith("--messages-per-producer=")) {
                messagesPerProducer = parseIntArg(arg, "--messages-per-producer=");
            } else if (arg.startsWith("--producer-count=")) {
                producerCount = parseIntArg(arg, "--producer-count=");
            } else if (arg.startsWith("--payload-size=")) {
                payloadSize = parseIntArg(arg, "--payload-size=");
            } else if (arg.startsWith("--topic=")) {
                topic = arg.substring("--topic=".length());
            } else if (arg.startsWith("--broker=")) {
                brokerAddress = arg.substring("--broker=".length());
            }
        }

        if (brokerAddress.isBlank()) {
            throw new IllegalArgumentException(
                    "Missing required broker. Pass --broker=tcp://<host>:1883. " +
                    "Examples: tcp://mqtt-broker:1883 for Kafka, tcp://pulsar:1883 for Pulsar."
            );
        }
        if (topic.isBlank()) {
            throw new IllegalArgumentException(
                    "Missing required topic. Pass --topic=<your/topic>. " +
                    "Examples: persistent://public/default/temp (Pulsar) or mqtt/temp (Kafka)."
            );
        }

        return new ProducerConfig(
                Math.max(0, delayBetweenMessagesMs),
                Math.max(1, messagesPerProducer),
                Math.max(1, producerCount),
                Math.max(130, payloadSize),
                topic,
                brokerAddress
        );
    }

    private static int parseIntArg(String arg, String prefix) {
        try {
            return Integer.parseInt(arg.substring(prefix.length()));
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException("Invalid value for " + prefix + " in arg: " + arg, ex);
        }
    }
}
