package org.example.schema;

import com.google.gson.Gson;
import org.apache.pulsar.functions.api.SerDe;
import java.nio.charset.StandardCharsets;

/** Decodes the MQTT handler's unstructured JSON bytes for the existing function. */
public final class SensorReadingTempJsonSerde implements SerDe<SensorReadingTemp> {
    private final Gson gson = new Gson();

    @Override
    public SensorReadingTemp deserialize(byte[] input) {
        return input == null ? null : gson.fromJson(new String(input, StandardCharsets.UTF_8), SensorReadingTemp.class);
    }

    @Override
    public byte[] serialize(SensorReadingTemp input) {
        return gson.toJson(input).getBytes(StandardCharsets.UTF_8);
    }
}
