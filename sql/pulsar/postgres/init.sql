CREATE TABLE pulsar_to_db_temp (
    message_id TEXT PRIMARY KEY,
    temperature DOUBLE PRECISION,
    timestamp TEXT,
    pulsar_timestamp TEXT,
    inserted_at TIMESTAMP DEFAULT CLOCK_TIMESTAMP(),
    payload TEXT
);

CREATE TABLE pulsar_processed_to_db_temp (
    "message_id" TEXT PRIMARY KEY,
    "producer" TEXT,
    "produced_timestamp" TEXT,
    "pulsar_timestamp" TEXT,
    "processed_timestamp" TEXT,
    "inserted_at" TIMESTAMP DEFAULT CLOCK_TIMESTAMP(),
    "temperature_fahrenheit" DOUBLE PRECISION,
    "updated_temperature_celsius" DOUBLE PRECISION,
    "payload" TEXT
);

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
    INOUT p99_producer_to_pulsar_ms DOUBLE PRECISION DEFAULT NULL,
    INOUT p95_producer_to_pulsar_ms DOUBLE PRECISION DEFAULT NULL,
    INOUT p90_producer_to_pulsar_ms DOUBLE PRECISION DEFAULT NULL,
    INOUT p50_producer_to_pulsar_ms DOUBLE PRECISION DEFAULT NULL,
    INOUT avg_producer_to_pulsar_ms DOUBLE PRECISION DEFAULT NULL,
    INOUT p99_pulsar_to_db_ms DOUBLE PRECISION DEFAULT NULL,
    INOUT p95_pulsar_to_db_ms DOUBLE PRECISION DEFAULT NULL,
    INOUT p90_pulsar_to_db_ms DOUBLE PRECISION DEFAULT NULL,
    INOUT p50_pulsar_to_db_ms DOUBLE PRECISION DEFAULT NULL,
    INOUT avg_pulsar_to_db_ms DOUBLE PRECISION DEFAULT NULL
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

        percentile_cont(0.99) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (pulsar_timestamp::timestamp(6) - timestamp::timestamp(6))) * 1000) AS p99_producer_to_pulsar_ms,
        percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (pulsar_timestamp::timestamp(6) - timestamp::timestamp(6))) * 1000) AS p95_producer_to_pulsar_ms,
        percentile_cont(0.90) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (pulsar_timestamp::timestamp(6) - timestamp::timestamp(6))) * 1000) AS p90_producer_to_pulsar_ms,
        percentile_cont(0.50) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (pulsar_timestamp::timestamp(6) - timestamp::timestamp(6))) * 1000) AS p50_producer_to_pulsar_ms,
        AVG(EXTRACT(EPOCH FROM (pulsar_timestamp::timestamp(6) - timestamp::timestamp(6))) * 1000) AS avg_producer_to_pulsar_ms,

        percentile_cont(0.99) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (inserted_at - pulsar_timestamp::timestamp(6))) * 1000) AS p99_pulsar_to_db_ms,
        percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (inserted_at - pulsar_timestamp::timestamp(6))) * 1000) AS p95_pulsar_to_db_ms,
        percentile_cont(0.90) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (inserted_at - pulsar_timestamp::timestamp(6))) * 1000) AS p90_pulsar_to_db_ms,
        percentile_cont(0.50) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (inserted_at - pulsar_timestamp::timestamp(6))) * 1000) AS p50_pulsar_to_db_ms,
        AVG(EXTRACT(EPOCH FROM (inserted_at - pulsar_timestamp::timestamp(6))) * 1000) AS avg_pulsar_to_db_ms
    INTO
        message_count,
        total_insertion_time_range_sec,
        p99_producer_to_db_ms,
        p95_producer_to_db_ms,
        p90_producer_to_db_ms,
        p50_producer_to_db_ms,
        avg_producer_to_db_ms,
        p99_producer_to_pulsar_ms,
        p95_producer_to_pulsar_ms,
        p90_producer_to_pulsar_ms,
        p50_producer_to_pulsar_ms,
        avg_producer_to_pulsar_ms,
        p99_pulsar_to_db_ms,
        p95_pulsar_to_db_ms,
        p90_pulsar_to_db_ms,
        p50_pulsar_to_db_ms,
        avg_pulsar_to_db_ms
    FROM pulsar_to_db_temp;
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
    INOUT p99_producer_to_pulsar_ms DOUBLE PRECISION DEFAULT NULL,
    INOUT p95_producer_to_pulsar_ms DOUBLE PRECISION DEFAULT NULL,
    INOUT p90_producer_to_pulsar_ms DOUBLE PRECISION DEFAULT NULL,
    INOUT p50_producer_to_pulsar_ms DOUBLE PRECISION DEFAULT NULL,
    INOUT avg_producer_to_pulsar_ms DOUBLE PRECISION DEFAULT NULL,
    INOUT p99_pulsar_to_processed_ms DOUBLE PRECISION DEFAULT NULL,
    INOUT p95_pulsar_to_processed_ms DOUBLE PRECISION DEFAULT NULL,
    INOUT p90_pulsar_to_processed_ms DOUBLE PRECISION DEFAULT NULL,
    INOUT p50_pulsar_to_processed_ms DOUBLE PRECISION DEFAULT NULL,
    INOUT avg_pulsar_to_processed_ms DOUBLE PRECISION DEFAULT NULL,
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
        EXTRACT(EPOCH FROM (MAX("inserted_at") - MIN("produced_timestamp"::timestamp(6)))) AS total_insertion_time_range_sec,

        percentile_cont(0.99) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM ("inserted_at" - "produced_timestamp"::timestamp(6))) * 1000) AS p99_producer_to_db_ms,
        percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM ("inserted_at" - "produced_timestamp"::timestamp(6))) * 1000) AS p95_producer_to_db_ms,
        percentile_cont(0.90) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM ("inserted_at" - "produced_timestamp"::timestamp(6))) * 1000) AS p90_producer_to_db_ms,
        percentile_cont(0.50) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM ("inserted_at" - "produced_timestamp"::timestamp(6))) * 1000) AS p50_producer_to_db_ms,
        AVG(EXTRACT(EPOCH FROM ("inserted_at" - "produced_timestamp"::timestamp(6))) * 1000) AS avg_producer_to_db_ms,

        percentile_cont(0.99) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM ("pulsar_timestamp"::timestamp(6) - "produced_timestamp"::timestamp(6))) * 1000) AS p99_producer_to_pulsar_ms,
        percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM ("pulsar_timestamp"::timestamp(6) - "produced_timestamp"::timestamp(6))) * 1000) AS p95_producer_to_pulsar_ms,
        percentile_cont(0.90) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM ("pulsar_timestamp"::timestamp(6) - "produced_timestamp"::timestamp(6))) * 1000) AS p90_producer_to_pulsar_ms,
        percentile_cont(0.50) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM ("pulsar_timestamp"::timestamp(6) - "produced_timestamp"::timestamp(6))) * 1000) AS p50_producer_to_pulsar_ms,
        AVG(EXTRACT(EPOCH FROM ("pulsar_timestamp"::timestamp(6) - "produced_timestamp"::timestamp(6))) * 1000) AS avg_producer_to_pulsar_ms,

        percentile_cont(0.99) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM ("processed_timestamp"::timestamp(6) - "pulsar_timestamp"::timestamp(6))) * 1000) AS p99_pulsar_to_processed_ms,
        percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM ("processed_timestamp"::timestamp(6) - "pulsar_timestamp"::timestamp(6))) * 1000) AS p95_pulsar_to_processed_ms,
        percentile_cont(0.90) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM ("processed_timestamp"::timestamp(6) - "pulsar_timestamp"::timestamp(6))) * 1000) AS p90_pulsar_to_processed_ms,
        percentile_cont(0.50) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM ("processed_timestamp"::timestamp(6) - "pulsar_timestamp"::timestamp(6))) * 1000) AS p50_pulsar_to_processed_ms,
        AVG(EXTRACT(EPOCH FROM ("processed_timestamp"::timestamp(6) - "pulsar_timestamp"::timestamp(6))) * 1000) AS avg_pulsar_to_processed_ms,

        percentile_cont(0.99) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM ("inserted_at" - "processed_timestamp"::timestamp(6))) * 1000) AS p99_processed_to_db_ms,
        percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM ("inserted_at" - "processed_timestamp"::timestamp(6))) * 1000) AS p95_processed_to_db_ms,
        percentile_cont(0.90) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM ("inserted_at" - "processed_timestamp"::timestamp(6))) * 1000) AS p90_processed_to_db_ms,
        percentile_cont(0.50) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM ("inserted_at" - "processed_timestamp"::timestamp(6))) * 1000) AS p50_processed_to_db_ms,
        AVG(EXTRACT(EPOCH FROM ("inserted_at" - "processed_timestamp"::timestamp(6))) * 1000) AS avg_processed_to_db_ms
    INTO
        message_count,
        total_insertion_time_range_sec,
        p99_producer_to_db_ms,
        p95_producer_to_db_ms,
        p90_producer_to_db_ms,
        p50_producer_to_db_ms,
        avg_producer_to_db_ms,
        p99_producer_to_pulsar_ms,
        p95_producer_to_pulsar_ms,
        p90_producer_to_pulsar_ms,
        p50_producer_to_pulsar_ms,
        avg_producer_to_pulsar_ms,
        p99_pulsar_to_processed_ms,
        p95_pulsar_to_processed_ms,
        p90_pulsar_to_processed_ms,
        p50_pulsar_to_processed_ms,
        avg_pulsar_to_processed_ms,
        p99_processed_to_db_ms,
        p95_processed_to_db_ms,
        p90_processed_to_db_ms,
        p50_processed_to_db_ms,
        avg_processed_to_db_ms
    FROM pulsar_processed_to_db_temp;
END;
$$;
