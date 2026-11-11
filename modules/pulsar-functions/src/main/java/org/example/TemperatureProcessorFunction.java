package org.example;

import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.example.schema.SensorReadingProcessedTemp;
import org.example.schema.SensorReadingTempSchema;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class TemperatureProcessorFunction implements Function<SensorReadingTempSchema, SensorReadingProcessedTemp> {
    private static final DateTimeFormatter FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS");

    @Override
    public SensorReadingProcessedTemp process(SensorReadingTempSchema input, Context context) {
        if (input == null) {
            return null;
        }

        double temperatureFahrenheit = input.getTemperature();
        double updatedTemperatureCelsius = (temperatureFahrenheit - 32.0d) * (5.0d / 9.0d);
        String processedTimestamp = LocalDateTime.now(ZoneOffset.UTC).format(FORMATTER);

        return new SensorReadingProcessedTemp(
                input.getMessage_id(),
                temperatureFahrenheit,
                updatedTemperatureCelsius,
                input.getTimestamp(),
                processedTimestamp,
                input.getPulsar_timestamp(),
                input.getPayload()
        );
    }
}
