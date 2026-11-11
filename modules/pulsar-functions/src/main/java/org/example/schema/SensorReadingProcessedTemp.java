package org.example.schema;

public class SensorReadingProcessedTemp {
    private String message_id;
    private double temperature_fahrenheit;
    private double updated_temperature_celsius;
    private String produced_timestamp;
    private String processed_timestamp;
    private String pulsar_timestamp;
    private String payload;

    public SensorReadingProcessedTemp() {
    }

    public SensorReadingProcessedTemp(
            String message_id,
            double temperature_fahrenheit,
            double updated_temperature_celsius,
            String produced_timestamp,
            String processed_timestamp,
            String pulsar_timestamp,
            String payload
    ) {
        this.message_id = message_id;
        this.temperature_fahrenheit = temperature_fahrenheit;
        this.updated_temperature_celsius = updated_temperature_celsius;
        this.produced_timestamp = produced_timestamp;
        this.processed_timestamp = processed_timestamp;
        this.pulsar_timestamp = pulsar_timestamp;
        this.payload = payload;
    }

    public String getMessage_id() {
        return message_id;
    }

    public void setMessage_id(String message_id) {
        this.message_id = message_id;
    }

    public double getTemperature_fahrenheit() {
        return temperature_fahrenheit;
    }

    public void setTemperature_fahrenheit(double temperature_fahrenheit) {
        this.temperature_fahrenheit = temperature_fahrenheit;
    }

    public double getUpdated_temperature_celsius() {
        return updated_temperature_celsius;
    }

    public void setUpdated_temperature_celsius(double updated_temperature_celsius) {
        this.updated_temperature_celsius = updated_temperature_celsius;
    }

    public String getProduced_timestamp() {
        return produced_timestamp;
    }

    public void setProduced_timestamp(String produced_timestamp) {
        this.produced_timestamp = produced_timestamp;
    }

    public String getProcessed_timestamp() {
        return processed_timestamp;
    }

    public void setProcessed_timestamp(String processed_timestamp) {
        this.processed_timestamp = processed_timestamp;
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
