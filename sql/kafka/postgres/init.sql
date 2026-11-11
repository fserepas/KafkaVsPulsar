CREATE TABLE IF NOT EXISTS kafka_to_db_temp (
    message_id TEXT PRIMARY KEY,
    temperature DOUBLE PRECISION,
    timestamp TEXT,
    kafka_timestamp TEXT,
    inserted_at TIMESTAMP DEFAULT CLOCK_TIMESTAMP(),
    payload TEXT
);
TRUNCATE TABLE kafka_to_db_temp;

CREATE TABLE IF NOT EXISTS kafka_processed_to_db_temp (
    "MESSAGE_ID" TEXT PRIMARY KEY,
    "PRODUCER" TEXT,
    "PRODUCED_TIMESTAMP" TEXT,
    "KAFKA_TIMESTAMP" TEXT,
    "PROCESSED_TIMESTAMP" TEXT,
    "INSERTED_AT" TIMESTAMP DEFAULT CLOCK_TIMESTAMP(),
    "TEMPERATURE_FAHRENHEIT" DOUBLE PRECISION,
    "UPDATED_TEMPERATURE_CELSIUS" DOUBLE PRECISION,
    "PAYLOAD" TEXT
);
TRUNCATE TABLE kafka_processed_to_db_temp;

DROP PROCEDURE IF EXISTS calculate_delays_raw_pipeline();
DROP PROCEDURE IF EXISTS calculate_delays_raw_pipeline(REFCURSOR);
CREATE OR REPLACE PROCEDURE calculate_delays_raw_pipeline(
    INOUT message_count BIGINT DEFAULT NULL,
    INOUT total_insertion_time_range_sec DOUBLE PRECISION DEFAULT NULL,
    INOUT p99_producer_to_db_ms DOUBLE PRECISION DEFAULT NULL,
    INOUT p95_producer_to_db_ms DOUBLE PRECISION DEFAULT NULL,
    INOUT p90_producer_to_db_ms DOUBLE PRECISION DEFAULT NULL,
    INOUT p50_producer_to_db_ms DOUBLE PRECISION DEFAULT NULL,
    INOUT avg_producer_to_db_ms DOUBLE PRECISION DEFAULT NULL,
    INOUT p99_producer_to_kafka_ms DOUBLE PRECISION DEFAULT NULL,
    INOUT p95_producer_to_kafka_ms DOUBLE PRECISION DEFAULT NULL,
    INOUT p90_producer_to_kafka_ms DOUBLE PRECISION DEFAULT NULL,
    INOUT p50_producer_to_kafka_ms DOUBLE PRECISION DEFAULT NULL,
    INOUT avg_producer_to_kafka_ms DOUBLE PRECISION DEFAULT NULL,
    INOUT p99_kafka_to_db_ms DOUBLE PRECISION DEFAULT NULL,
    INOUT p95_kafka_to_db_ms DOUBLE PRECISION DEFAULT NULL,
    INOUT p90_kafka_to_db_ms DOUBLE PRECISION DEFAULT NULL,
    INOUT p50_kafka_to_db_ms DOUBLE PRECISION DEFAULT NULL,
    INOUT avg_kafka_to_db_ms DOUBLE PRECISION DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
BEGIN
    SELECT
        COUNT(*) AS message_count,
        EXTRACT(EPOCH FROM (MAX(inserted_at) - MIN(timestamp::timestamp(6)))) AS total_insertion_time_range_sec,

        percentile_cont(0.99) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (inserted_at - timestamp::timestamp(6))) * 1000) AS p99_producer_to_db_ms,
        percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (inserted_at - timestamp::timestamp(6))) * 1000) AS p95_producer_to_db_ms,
        percentile_cont(0.90) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (inserted_at - timestamp::timestamp(6))) * 1000) AS p90_producer_to_db_ms,
        percentile_cont(0.50) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (inserted_at - timestamp::timestamp(6))) * 1000) AS p50_producer_to_db_ms,
        AVG(EXTRACT(EPOCH FROM (inserted_at - timestamp::timestamp(6))) * 1000) AS avg_producer_to_db_ms,

        percentile_cont(0.99) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (kafka_timestamp::timestamp(6) - timestamp::timestamp(6))) * 1000) AS p99_producer_to_kafka_ms,
        percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (kafka_timestamp::timestamp(6) - timestamp::timestamp(6))) * 1000) AS p95_producer_to_kafka_ms,
        percentile_cont(0.90) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (kafka_timestamp::timestamp(6) - timestamp::timestamp(6))) * 1000) AS p90_producer_to_kafka_ms,
        percentile_cont(0.50) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (kafka_timestamp::timestamp(6) - timestamp::timestamp(6))) * 1000) AS p50_producer_to_kafka_ms,
        AVG(EXTRACT(EPOCH FROM (kafka_timestamp::timestamp(6) - timestamp::timestamp(6))) * 1000) AS avg_producer_to_kafka_ms,

        percentile_cont(0.99) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (inserted_at - kafka_timestamp::timestamp(6))) * 1000) AS p99_kafka_to_db_ms,
        percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (inserted_at - kafka_timestamp::timestamp(6))) * 1000) AS p95_kafka_to_db_ms,
        percentile_cont(0.90) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (inserted_at - kafka_timestamp::timestamp(6))) * 1000) AS p90_kafka_to_db_ms,
        percentile_cont(0.50) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (inserted_at - kafka_timestamp::timestamp(6))) * 1000) AS p50_kafka_to_db_ms,
        AVG(EXTRACT(EPOCH FROM (inserted_at - kafka_timestamp::timestamp(6))) * 1000) AS avg_kafka_to_db_ms
    INTO
        message_count,
        total_insertion_time_range_sec,
        p99_producer_to_db_ms,
        p95_producer_to_db_ms,
        p90_producer_to_db_ms,
        p50_producer_to_db_ms,
        avg_producer_to_db_ms,
        p99_producer_to_kafka_ms,
        p95_producer_to_kafka_ms,
        p90_producer_to_kafka_ms,
        p50_producer_to_kafka_ms,
        avg_producer_to_kafka_ms,
        p99_kafka_to_db_ms,
        p95_kafka_to_db_ms,
        p90_kafka_to_db_ms,
        p50_kafka_to_db_ms,
        avg_kafka_to_db_ms
    FROM kafka_to_db_temp;
END;
$$;

DROP PROCEDURE IF EXISTS calculate_delays_processed_pipeline();
DROP PROCEDURE IF EXISTS calculate_delays_processed_pipeline(REFCURSOR);
CREATE OR REPLACE PROCEDURE calculate_delays_processed_pipeline(
    INOUT message_count BIGINT DEFAULT NULL,
    INOUT total_insertion_time_range_sec DOUBLE PRECISION DEFAULT NULL,
    INOUT p99_producer_to_db_ms DOUBLE PRECISION DEFAULT NULL,
    INOUT p95_producer_to_db_ms DOUBLE PRECISION DEFAULT NULL,
    INOUT p90_producer_to_db_ms DOUBLE PRECISION DEFAULT NULL,
    INOUT p50_producer_to_db_ms DOUBLE PRECISION DEFAULT NULL,
    INOUT avg_producer_to_db_ms DOUBLE PRECISION DEFAULT NULL,
    INOUT p99_producer_to_kafka_ms DOUBLE PRECISION DEFAULT NULL,
    INOUT p95_producer_to_kafka_ms DOUBLE PRECISION DEFAULT NULL,
    INOUT p90_producer_to_kafka_ms DOUBLE PRECISION DEFAULT NULL,
    INOUT p50_producer_to_kafka_ms DOUBLE PRECISION DEFAULT NULL,
    INOUT avg_producer_to_kafka_ms DOUBLE PRECISION DEFAULT NULL,
    INOUT p99_kafka_to_processed_ms DOUBLE PRECISION DEFAULT NULL,
    INOUT p95_kafka_to_processed_ms DOUBLE PRECISION DEFAULT NULL,
    INOUT p90_kafka_to_processed_ms DOUBLE PRECISION DEFAULT NULL,
    INOUT p50_kafka_to_processed_ms DOUBLE PRECISION DEFAULT NULL,
    INOUT avg_kafka_to_processed_ms DOUBLE PRECISION DEFAULT NULL,
    INOUT p99_processed_to_db_ms DOUBLE PRECISION DEFAULT NULL,
    INOUT p95_processed_to_db_ms DOUBLE PRECISION DEFAULT NULL,
    INOUT p90_processed_to_db_ms DOUBLE PRECISION DEFAULT NULL,
    INOUT p50_processed_to_db_ms DOUBLE PRECISION DEFAULT NULL,
    INOUT avg_processed_to_db_ms DOUBLE PRECISION DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
BEGIN
    SELECT
        COUNT(*) AS message_count,
        EXTRACT(EPOCH FROM (MAX("INSERTED_AT") - MIN("PRODUCED_TIMESTAMP"::timestamp(6)))) AS total_insertion_time_range_sec,

        percentile_cont(0.99) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM ("INSERTED_AT" - "PRODUCED_TIMESTAMP"::timestamp(6))) * 1000) AS p99_producer_to_db_ms,
        percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM ("INSERTED_AT" - "PRODUCED_TIMESTAMP"::timestamp(6))) * 1000) AS p95_producer_to_db_ms,
        percentile_cont(0.90) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM ("INSERTED_AT" - "PRODUCED_TIMESTAMP"::timestamp(6))) * 1000) AS p90_producer_to_db_ms,
        percentile_cont(0.50) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM ("INSERTED_AT" - "PRODUCED_TIMESTAMP"::timestamp(6))) * 1000) AS p50_producer_to_db_ms,
        AVG(EXTRACT(EPOCH FROM ("INSERTED_AT" - "PRODUCED_TIMESTAMP"::timestamp(6))) * 1000) AS avg_producer_to_db_ms,

        percentile_cont(0.99) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM ("KAFKA_TIMESTAMP"::timestamp(6) - "PRODUCED_TIMESTAMP"::timestamp(6))) * 1000) AS p99_producer_to_kafka_ms,
        percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM ("KAFKA_TIMESTAMP"::timestamp(6) - "PRODUCED_TIMESTAMP"::timestamp(6))) * 1000) AS p95_producer_to_kafka_ms,
        percentile_cont(0.90) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM ("KAFKA_TIMESTAMP"::timestamp(6) - "PRODUCED_TIMESTAMP"::timestamp(6))) * 1000) AS p90_producer_to_kafka_ms,
        percentile_cont(0.50) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM ("KAFKA_TIMESTAMP"::timestamp(6) - "PRODUCED_TIMESTAMP"::timestamp(6))) * 1000) AS p50_producer_to_kafka_ms,
        AVG(EXTRACT(EPOCH FROM ("KAFKA_TIMESTAMP"::timestamp(6) - "PRODUCED_TIMESTAMP"::timestamp(6))) * 1000) AS avg_producer_to_kafka_ms,

        percentile_cont(0.99) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM ("PROCESSED_TIMESTAMP"::timestamp(6) - "KAFKA_TIMESTAMP"::timestamp(6))) * 1000) AS p99_kafka_to_processed_ms,
        percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM ("PROCESSED_TIMESTAMP"::timestamp(6) - "KAFKA_TIMESTAMP"::timestamp(6))) * 1000) AS p95_kafka_to_processed_ms,
        percentile_cont(0.90) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM ("PROCESSED_TIMESTAMP"::timestamp(6) - "KAFKA_TIMESTAMP"::timestamp(6))) * 1000) AS p90_kafka_to_processed_ms,
        percentile_cont(0.50) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM ("PROCESSED_TIMESTAMP"::timestamp(6) - "KAFKA_TIMESTAMP"::timestamp(6))) * 1000) AS p50_kafka_to_processed_ms,
        AVG(EXTRACT(EPOCH FROM ("PROCESSED_TIMESTAMP"::timestamp(6) - "KAFKA_TIMESTAMP"::timestamp(6))) * 1000) AS avg_kafka_to_processed_ms,

        percentile_cont(0.99) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM ("INSERTED_AT" - "PROCESSED_TIMESTAMP"::timestamp(6))) * 1000) AS p99_processed_to_db_ms,
        percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM ("INSERTED_AT" - "PROCESSED_TIMESTAMP"::timestamp(6))) * 1000) AS p95_processed_to_db_ms,
        percentile_cont(0.90) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM ("INSERTED_AT" - "PROCESSED_TIMESTAMP"::timestamp(6))) * 1000) AS p90_processed_to_db_ms,
        percentile_cont(0.50) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM ("INSERTED_AT" - "PROCESSED_TIMESTAMP"::timestamp(6))) * 1000) AS p50_processed_to_db_ms,
        AVG(EXTRACT(EPOCH FROM ("INSERTED_AT" - "PROCESSED_TIMESTAMP"::timestamp(6))) * 1000) AS avg_processed_to_db_ms
    INTO
        message_count,
        total_insertion_time_range_sec,
        p99_producer_to_db_ms,
        p95_producer_to_db_ms,
        p90_producer_to_db_ms,
        p50_producer_to_db_ms,
        avg_producer_to_db_ms,
        p99_producer_to_kafka_ms,
        p95_producer_to_kafka_ms,
        p90_producer_to_kafka_ms,
        p50_producer_to_kafka_ms,
        avg_producer_to_kafka_ms,
        p99_kafka_to_processed_ms,
        p95_kafka_to_processed_ms,
        p90_kafka_to_processed_ms,
        p50_kafka_to_processed_ms,
        avg_kafka_to_processed_ms,
        p99_processed_to_db_ms,
        p95_processed_to_db_ms,
        p90_processed_to_db_ms,
        p50_processed_to_db_ms,
        avg_processed_to_db_ms
    FROM kafka_processed_to_db_temp;
END;
$$;
