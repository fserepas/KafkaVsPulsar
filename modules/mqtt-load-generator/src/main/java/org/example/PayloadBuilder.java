package org.example;

public final class PayloadBuilder {
    private static final int MINIMUM_MESSAGE_BYTES = 130;

    private PayloadBuilder() {
    }

    public static String build(int payloadSize) {
        int fillSize = Math.max(0, payloadSize - MINIMUM_MESSAGE_BYTES);
        return "A".repeat(fillSize);
    }
}
