package org.example.schema;

public class SensorReadingTempSchema {
    private String message_id;
    private double temperature;
    private String timestamp;
    private String pulsar_timestamp;
    private String payload;

    public SensorReadingTempSchema() {
    }

    public SensorReadingTempSchema(
            String message_id,
            double temperature,
            String timestamp,
            String pulsar_timestamp,
            String payload
    ) {
        this.message_id = message_id;
        this.temperature = temperature;
        this.timestamp = timestamp;
        this.pulsar_timestamp = pulsar_timestamp;
        this.payload = payload;
    }

    public String getMessage_id() {
        return message_id;
    }

    public void setMessage_id(String message_id) {
        this.message_id = message_id;
    }

    public double getTemperature() {
        return temperature;
    }

    public void setTemperature(double temperature) {
        this.temperature = temperature;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getPulsar_timestamp() {
        return pulsar_timestamp;
    }

    public void setPulsar_timestamp(String pulsar_timestamp) {
        this.pulsar_timestamp = pulsar_timestamp;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }
}
