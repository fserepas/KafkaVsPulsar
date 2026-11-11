CREATE TABLE IF NOT EXISTS `kafka_to_db_temp` (
  `message_id` VARCHAR(255) PRIMARY KEY,
  `timestamp` TEXT,
  `kafka_timestamp` TEXT,
  `inserted_at` TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  `temperature` DOUBLE,
  `payload` TEXT
);
TRUNCATE TABLE `kafka_to_db_temp`;

CREATE TABLE IF NOT EXISTS `kafka_processed_to_db_temp` (
    `MESSAGE_ID` VARCHAR(255) PRIMARY KEY,
    `PRODUCER` TEXT,
    `PRODUCED_TIMESTAMP` TEXT,
    `KAFKA_TIMESTAMP` TEXT,
    `PROCESSED_TIMESTAMP` TEXT,
    `INSERTED_AT` TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    `TEMPERATURE_FAHRENHEIT` DOUBLE PRECISION,
    `UPDATED_TEMPERATURE_CELSIUS` DOUBLE PRECISION,
    `PAYLOAD` TEXT
);
TRUNCATE TABLE `kafka_processed_to_db_temp`;

DELIMITER //

CREATE PROCEDURE calculate_delays_raw_pipeline()
BEGIN
  WITH base AS (
    SELECT
      STR_TO_DATE(LEFT(REPLACE(`timestamp`, 'T', ' '), 26), '%Y-%m-%d %H:%i:%s.%f') AS producer_ts,
      STR_TO_DATE(LEFT(REPLACE(`kafka_timestamp`, 'T', ' '), 26), '%Y-%m-%d %H:%i:%s.%f') AS kafka_ts,
      inserted_at
    FROM kafka_to_db_temp
    WHERE `timestamp` IS NOT NULL
      AND kafka_timestamp IS NOT NULL
      AND inserted_at IS NOT NULL
  ),
  delays AS (
    SELECT
      *,
      TIMESTAMPDIFF(MICROSECOND, producer_ts, kafka_ts) / 1000.0 AS producer_to_kafka_ms,
      TIMESTAMPDIFF(MICROSECOND, producer_ts, inserted_at) / 1000.0 AS producer_to_db_ms,
      TIMESTAMPDIFF(MICROSECOND, kafka_ts, inserted_at) / 1000.0 AS kafka_to_db_ms
    FROM base
  ),
  ranked AS (
    SELECT
      *,
      ROW_NUMBER() OVER (ORDER BY producer_to_db_ms) AS rn_prod_db,
      ROW_NUMBER() OVER (ORDER BY producer_to_kafka_ms) AS rn_prod_kafka,
      ROW_NUMBER() OVER (ORDER BY kafka_to_db_ms) AS rn_kafka_db,
      COUNT(*) OVER () AS cnt
    FROM delays
  )
  SELECT
    MAX(cnt) AS message_count,

    TIMESTAMPDIFF(MICROSECOND, MIN(producer_ts), MAX(inserted_at)) / 1000000.0 AS total_insertion_time_range_sec,

    MAX(CASE WHEN rn_prod_db = CEIL(0.99 * cnt) THEN producer_to_db_ms END) AS p99_producer_to_db_ms,
    MAX(CASE WHEN rn_prod_db = CEIL(0.95 * cnt) THEN producer_to_db_ms END) AS p95_producer_to_db_ms,
    MAX(CASE WHEN rn_prod_db = CEIL(0.90 * cnt) THEN producer_to_db_ms END) AS p90_producer_to_db_ms,
    MAX(CASE WHEN rn_prod_db = CEIL(0.50 * cnt) THEN producer_to_db_ms END) AS p50_producer_to_db_ms,
    AVG(producer_to_db_ms) AS avg_delay_producer_to_db_ms,

    MAX(CASE WHEN rn_prod_kafka = CEIL(0.99 * cnt) THEN producer_to_kafka_ms END) AS p99_producer_to_kafka_ms,
    MAX(CASE WHEN rn_prod_kafka = CEIL(0.95 * cnt) THEN producer_to_kafka_ms END) AS p95_producer_to_kafka_ms,
    MAX(CASE WHEN rn_prod_kafka = CEIL(0.90 * cnt) THEN producer_to_kafka_ms END) AS p90_producer_to_kafka_ms,
    MAX(CASE WHEN rn_prod_kafka = CEIL(0.50 * cnt) THEN producer_to_kafka_ms END) AS p50_producer_to_kafka_ms,
    AVG(producer_to_kafka_ms) AS avg_delay_producer_to_kafka_ms,

    MAX(CASE WHEN rn_kafka_db = CEIL(0.99 * cnt) THEN kafka_to_db_ms END) AS p99_kafka_to_db_ms,
    MAX(CASE WHEN rn_kafka_db = CEIL(0.95 * cnt) THEN kafka_to_db_ms END) AS p95_kafka_to_db_ms,
    MAX(CASE WHEN rn_kafka_db = CEIL(0.90 * cnt) THEN kafka_to_db_ms END) AS p90_kafka_to_db_ms,
    MAX(CASE WHEN rn_kafka_db = CEIL(0.50 * cnt) THEN kafka_to_db_ms END) AS p50_kafka_to_db_ms,
    AVG(kafka_to_db_ms) AS avg_delay_kafka_to_db_ms
  FROM ranked;
END //

CREATE PROCEDURE calculate_delays_processed_pipeline()
BEGIN
  WITH base AS (
    SELECT
      STR_TO_DATE(LEFT(REPLACE(`PRODUCED_TIMESTAMP`, 'T', ' '), 26), '%Y-%m-%d %H:%i:%s.%f') AS produced_ts,
      STR_TO_DATE(LEFT(REPLACE(`KAFKA_TIMESTAMP`, 'T', ' '), 26), '%Y-%m-%d %H:%i:%s.%f') AS kafka_ts,
      STR_TO_DATE(LEFT(REPLACE(`PROCESSED_TIMESTAMP`, 'T', ' '), 26), '%Y-%m-%d %H:%i:%s.%f') AS processed_ts,
      `INSERTED_AT` AS inserted_at
    FROM kafka_processed_to_db_temp
    WHERE `PRODUCED_TIMESTAMP` IS NOT NULL
      AND `KAFKA_TIMESTAMP` IS NOT NULL
      AND `PROCESSED_TIMESTAMP` IS NOT NULL
      AND `INSERTED_AT` IS NOT NULL
  ),
  delays AS (
    SELECT
      *,
      TIMESTAMPDIFF(MICROSECOND, produced_ts, inserted_at) / 1000.0 AS producer_to_db_ms,
      TIMESTAMPDIFF(MICROSECOND, produced_ts, kafka_ts) / 1000.0 AS producer_to_kafka_ms,
      TIMESTAMPDIFF(MICROSECOND, kafka_ts, processed_ts) / 1000.0 AS kafka_to_processed_ms,
      TIMESTAMPDIFF(MICROSECOND, processed_ts, inserted_at) / 1000.0 AS processed_to_db_ms
    FROM base
  ),
  ranked AS (
    SELECT
      *,
      ROW_NUMBER() OVER (ORDER BY producer_to_kafka_ms) AS rn_prod_kafka,
      ROW_NUMBER() OVER (ORDER BY producer_to_db_ms) AS rn_prod_db,
      ROW_NUMBER() OVER (ORDER BY kafka_to_processed_ms) AS rn_kafka_processed,
      ROW_NUMBER() OVER (ORDER BY processed_to_db_ms) AS rn_processed_db,
      COUNT(*) OVER () AS cnt
    FROM delays
  )
  SELECT
    MAX(cnt) AS message_count,
    TIMESTAMPDIFF(MICROSECOND, MIN(produced_ts), MAX(inserted_at)) / 1000000.0 AS total_insertion_time_range_sec,

    MAX(CASE WHEN rn_prod_kafka = CEIL(0.99 * cnt) THEN producer_to_kafka_ms END) AS p99_producer_to_kafka_ms,
    MAX(CASE WHEN rn_prod_kafka = CEIL(0.95 * cnt) THEN producer_to_kafka_ms END) AS p95_producer_to_kafka_ms,
    MAX(CASE WHEN rn_prod_kafka = CEIL(0.90 * cnt) THEN producer_to_kafka_ms END) AS p90_producer_to_kafka_ms,
    MAX(CASE WHEN rn_prod_kafka = CEIL(0.50 * cnt) THEN producer_to_kafka_ms END) AS p50_producer_to_kafka_ms,
    AVG(producer_to_kafka_ms) AS avg_delay_producer_to_kafka_ms,

    MAX(CASE WHEN rn_prod_db = CEIL(0.99 * cnt) THEN producer_to_db_ms END) AS p99_producer_to_db_ms,
    MAX(CASE WHEN rn_prod_db = CEIL(0.95 * cnt) THEN producer_to_db_ms END) AS p95_producer_to_db_ms,
    MAX(CASE WHEN rn_prod_db = CEIL(0.90 * cnt) THEN producer_to_db_ms END) AS p90_producer_to_db_ms,
    MAX(CASE WHEN rn_prod_db = CEIL(0.50 * cnt) THEN producer_to_db_ms END) AS p50_producer_to_db_ms,
    AVG(producer_to_db_ms) AS avg_delay_producer_to_db_ms,

    MAX(CASE WHEN rn_kafka_processed = CEIL(0.99 * cnt) THEN kafka_to_processed_ms END) AS p99_kafka_to_processed_ms,
    MAX(CASE WHEN rn_kafka_processed = CEIL(0.95 * cnt) THEN kafka_to_processed_ms END) AS p95_kafka_to_processed_ms,
    MAX(CASE WHEN rn_kafka_processed = CEIL(0.90 * cnt) THEN kafka_to_processed_ms END) AS p90_kafka_to_processed_ms,
    MAX(CASE WHEN rn_kafka_processed = CEIL(0.50 * cnt) THEN kafka_to_processed_ms END) AS p50_kafka_to_processed_ms,
    AVG(kafka_to_processed_ms) AS avg_kafka_to_processed_ms,

    MAX(CASE WHEN rn_processed_db = CEIL(0.99 * cnt) THEN processed_to_db_ms END) AS p99_processed_to_db_ms,
    MAX(CASE WHEN rn_processed_db = CEIL(0.95 * cnt) THEN processed_to_db_ms END) AS p95_processed_to_db_ms,
    MAX(CASE WHEN rn_processed_db = CEIL(0.90 * cnt) THEN processed_to_db_ms END) AS p90_processed_to_db_ms,
    MAX(CASE WHEN rn_processed_db = CEIL(0.50 * cnt) THEN processed_to_db_ms END) AS p50_processed_to_db_ms,
    AVG(processed_to_db_ms) AS avg_processed_to_db_ms
  FROM ranked;
END //

DELIMITER ;
