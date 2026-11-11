package org.example;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

// Only non-null fields
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ProcessedTemp {

    @JsonProperty("PRODUCER")
    public String PRODUCER;

    @JsonProperty("MESSAGE_ID")
    public String MESSAGE_ID;

    @JsonProperty("TEMPERATURE_FAHRENHEIT")
    public Double TEMPERATURE_FAHRENHEIT;

    @JsonProperty("UPDATED_TEMPERATURE_CELSIUS")
    public Double UPDATED_TEMPERATURE_CELSIUS;

    @JsonProperty("PRODUCED_TIMESTAMP")
    public String PRODUCED_TIMESTAMP;

    @JsonProperty("KAFKA_TIMESTAMP")
    public String KAFKA_TIMESTAMP;

    @JsonProperty("PROCESSED_TIMESTAMP")
    public String PROCESSED_TIMESTAMP;

    @JsonProperty("PAYLOAD")
    public String PAYLOAD;

    public ProcessedTemp() {}
}