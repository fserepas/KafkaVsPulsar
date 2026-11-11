CREATE STREAM temp_stream (
    producer_id STRING KEY,
    payload STRUCT<
        message_id STRING,
        temperature DOUBLE,
        timestamp STRING,
        kafka_timestamp STRING,
        payload STRING
    >
) WITH (
    KAFKA_TOPIC = 'temp',
    VALUE_FORMAT = 'JSON',
    PARTITIONS = 1
);

CREATE STREAM processed_ksqldb_temp WITH (
    KAFKA_TOPIC = 'processed_ksqldb_temp',
    VALUE_FORMAT = 'JSON_SR',
    PARTITIONS = 1
) AS
SELECT
    producer_id,
    AS_VALUE(producer_id) AS producer,
    payload->message_id AS message_id,
    payload->temperature AS temperature_fahrenheit,
    ((payload->temperature - 32) * 5 / 9) AS updated_temperature_Celsius,
    payload->timestamp AS produced_timestamp,
    payload->kafka_timestamp AS kafka_timestamp,
    FORMAT_TIMESTAMP(FROM_UNIXTIME(UNIX_TIMESTAMP()), 'yyyy-MM-dd''T''HH:mm:ss.SSSSSSSSS') AS processed_timestamp,
    payload->payload AS payload
FROM temp_stream
PARTITION BY producer_id;
