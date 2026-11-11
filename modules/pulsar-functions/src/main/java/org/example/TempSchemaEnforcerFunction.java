package org.example;

import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.example.schema.SensorReadingTemp;
import org.example.schema.SensorReadingTempSchema;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class TempSchemaEnforcerFunction implements Function<SensorReadingTemp, SensorReadingTempSchema> {
    private static final DateTimeFormatter FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS");

    @Override
    public SensorReadingTempSchema process(SensorReadingTemp input, Context context) {
        if (input == null) {
            return null;
        }

        long publishTimeMillis = context.getCurrentRecord()
                .getMessage()
                .map(message -> message.getPublishTime())
                .orElse(System.currentTimeMillis());

        String pulsarTimestamp = LocalDateTime.ofInstant(
                Instant.ofEpochMilli(publishTimeMillis),
                ZoneOffset.UTC
        ).format(FORMATTER);

        return new SensorReadingTempSchema(
                input.getMessage_id(),
                input.getTemperature(),
                input.getTimestamp(),
                pulsarTimestamp,
                input.getPayload()
        );
    }

}
