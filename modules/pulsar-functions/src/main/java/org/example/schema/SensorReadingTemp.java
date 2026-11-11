package org.example.schema;

public class SensorReadingTemp {
    private String message_id;
    private double temperature;
    private String timestamp;
    private String payload;

    public SensorReadingTemp() {
    }

    public SensorReadingTemp(String message_id, double temperature, String timestamp, String payload) {
        this.message_id = message_id;
        this.temperature = temperature;
        this.timestamp = timestamp;
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

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }
}
