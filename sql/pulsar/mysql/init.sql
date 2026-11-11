CREATE TABLE IF NOT EXISTS `pulsar_to_db_temp` (
  `message_id` VARCHAR(255) PRIMARY KEY,
  `timestamp` TEXT,
  `pulsar_timestamp` TEXT,
  `inserted_at` TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  `temperature` DOUBLE
);
TRUNCATE TABLE `pulsar_to_db_temp`;

CREATE TABLE IF NOT EXISTS `pulsar_processed_to_db_temp` (
  `message_id` VARCHAR(255) PRIMARY KEY,
  `produced_timestamp` TEXT,
  `pulsar_timestamp` TEXT,
  `processed_timestamp` TEXT,
  `inserted_at` TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  `temperature_fahrenheit` DOUBLE,
  `updated_temperature_celsius` DOUBLE,
  `payload` TEXT
);
TRUNCATE TABLE `pulsar_processed_to_db_temp`;

DELIMITER //

CREATE PROCEDURE calculate_delays_raw_pipeline()
BEGIN
  WITH base AS (
    SELECT
      STR_TO_DATE(LEFT(REPLACE(`timestamp`, 'T', ' '), 26), '%Y-%m-%d %H:%i:%s.%f') AS producer_ts,
      STR_TO_DATE(LEFT(REPLACE(`pulsar_timestamp`, 'T', ' '), 26), '%Y-%m-%d %H:%i:%s.%f') AS pulsar_ts,
      inserted_at
    FROM pulsar_to_db_temp
    WHERE `timestamp` IS NOT NULL
      AND pulsar_timestamp IS NOT NULL
      AND inserted_at IS NOT NULL
  ),
  delays AS (
    SELECT
      *,
      TIMESTAMPDIFF(MICROSECOND, producer_ts, pulsar_ts) / 1000.0 AS producer_to_pulsar_ms,
      TIMESTAMPDIFF(MICROSECOND, producer_ts, inserted_at) / 1000.0 AS producer_to_db_ms,
      TIMESTAMPDIFF(MICROSECOND, pulsar_ts, inserted_at) / 1000.0 AS pulsar_to_db_ms
    FROM base
  ),
  ranked AS (
    SELECT
      *,
      ROW_NUMBER() OVER (ORDER BY producer_to_db_ms) AS rn_prod_db,
      ROW_NUMBER() OVER (ORDER BY producer_to_pulsar_ms) AS rn_prod_pulsar,
      ROW_NUMBER() OVER (ORDER BY pulsar_to_db_ms) AS rn_pulsar_db,
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

    MAX(CASE WHEN rn_prod_pulsar = CEIL(0.99 * cnt) THEN producer_to_pulsar_ms END) AS p99_producer_to_pulsar_ms,
    MAX(CASE WHEN rn_prod_pulsar = CEIL(0.95 * cnt) THEN producer_to_pulsar_ms END) AS p95_producer_to_pulsar_ms,
    MAX(CASE WHEN rn_prod_pulsar = CEIL(0.90 * cnt) THEN producer_to_pulsar_ms END) AS p90_producer_to_pulsar_ms,
    MAX(CASE WHEN rn_prod_pulsar = CEIL(0.50 * cnt) THEN producer_to_pulsar_ms END) AS p50_producer_to_pulsar_ms,
    AVG(producer_to_pulsar_ms) AS avg_delay_producer_to_pulsar_ms,

    MAX(CASE WHEN rn_pulsar_db = CEIL(0.99 * cnt) THEN pulsar_to_db_ms END) AS p99_pulsar_to_db_ms,
    MAX(CASE WHEN rn_pulsar_db = CEIL(0.95 * cnt) THEN pulsar_to_db_ms END) AS p95_pulsar_to_db_ms,
    MAX(CASE WHEN rn_pulsar_db = CEIL(0.90 * cnt) THEN pulsar_to_db_ms END) AS p90_pulsar_to_db_ms,
    MAX(CASE WHEN rn_pulsar_db = CEIL(0.50 * cnt) THEN pulsar_to_db_ms END) AS p50_pulsar_to_db_ms,
    AVG(pulsar_to_db_ms) AS avg_delay_pulsar_to_db_ms
  FROM ranked;
END //

CREATE PROCEDURE calculate_delays_processed_pipeline()
BEGIN
  WITH base AS (
    SELECT
      STR_TO_DATE(LEFT(REPLACE(`produced_timestamp`, 'T', ' '), 26), '%Y-%m-%d %H:%i:%s.%f') AS produced_ts,
      STR_TO_DATE(LEFT(REPLACE(`pulsar_timestamp`, 'T', ' '), 26), '%Y-%m-%d %H:%i:%s.%f') AS pulsar_ts,
      STR_TO_DATE(LEFT(REPLACE(`processed_timestamp`, 'T', ' '), 26), '%Y-%m-%d %H:%i:%s.%f') AS processed_ts,
      `inserted_at` AS inserted_at
    FROM pulsar_processed_to_db_temp
    WHERE `produced_timestamp` IS NOT NULL
      AND `pulsar_timestamp` IS NOT NULL
      AND `processed_timestamp` IS NOT NULL
      AND `inserted_at` IS NOT NULL
  ),
  delays AS (
    SELECT
      *,
      TIMESTAMPDIFF(MICROSECOND, produced_ts, inserted_at) / 1000.0 AS producer_to_db_ms,
      TIMESTAMPDIFF(MICROSECOND, produced_ts, pulsar_ts) / 1000.0 AS producer_to_pulsar_ms,
      TIMESTAMPDIFF(MICROSECOND, pulsar_ts, processed_ts) / 1000.0 AS pulsar_to_processed_ms,
      TIMESTAMPDIFF(MICROSECOND, processed_ts, inserted_at) / 1000.0 AS processed_to_db_ms
    FROM base
  ),
  ranked AS (
    SELECT
      *,
      ROW_NUMBER() OVER (ORDER BY producer_to_pulsar_ms) AS rn_prod_pulsar,
      ROW_NUMBER() OVER (ORDER BY producer_to_db_ms) AS rn_prod_db,
      ROW_NUMBER() OVER (ORDER BY pulsar_to_processed_ms) AS rn_pulsar_processed,
      ROW_NUMBER() OVER (ORDER BY processed_to_db_ms) AS rn_processed_db,
      COUNT(*) OVER () AS cnt
    FROM delays
  )
  SELECT
    MAX(cnt) AS message_count,
    TIMESTAMPDIFF(MICROSECOND, MIN(produced_ts), MAX(inserted_at)) / 1000000.0 AS total_insertion_time_range_sec,

    MAX(CASE WHEN rn_prod_pulsar = CEIL(0.99 * cnt) THEN producer_to_pulsar_ms END) AS p99_producer_to_pulsar_ms,
    MAX(CASE WHEN rn_prod_pulsar = CEIL(0.95 * cnt) THEN producer_to_pulsar_ms END) AS p95_producer_to_pulsar_ms,
    MAX(CASE WHEN rn_prod_pulsar = CEIL(0.90 * cnt) THEN producer_to_pulsar_ms END) AS p90_producer_to_pulsar_ms,
    MAX(CASE WHEN rn_prod_pulsar = CEIL(0.50 * cnt) THEN producer_to_pulsar_ms END) AS p50_producer_to_pulsar_ms,
    AVG(producer_to_pulsar_ms) AS avg_delay_producer_to_pulsar_ms,

    MAX(CASE WHEN rn_prod_db = CEIL(0.99 * cnt) THEN producer_to_db_ms END) AS p99_producer_to_db_ms,
    MAX(CASE WHEN rn_prod_db = CEIL(0.95 * cnt) THEN producer_to_db_ms END) AS p95_producer_to_db_ms,
    MAX(CASE WHEN rn_prod_db = CEIL(0.90 * cnt) THEN producer_to_db_ms END) AS p90_producer_to_db_ms,
    MAX(CASE WHEN rn_prod_db = CEIL(0.50 * cnt) THEN producer_to_db_ms END) AS p50_producer_to_db_ms,
    AVG(producer_to_db_ms) AS avg_delay_producer_to_db_ms,

    MAX(CASE WHEN rn_pulsar_processed = CEIL(0.99 * cnt) THEN pulsar_to_processed_ms END) AS p99_pulsar_to_processed_ms,
    MAX(CASE WHEN rn_pulsar_processed = CEIL(0.95 * cnt) THEN pulsar_to_processed_ms END) AS p95_pulsar_to_processed_ms,
    MAX(CASE WHEN rn_pulsar_processed = CEIL(0.90 * cnt) THEN pulsar_to_processed_ms END) AS p90_pulsar_to_processed_ms,
    MAX(CASE WHEN rn_pulsar_processed = CEIL(0.50 * cnt) THEN pulsar_to_processed_ms END) AS p50_pulsar_to_processed_ms,
    AVG(pulsar_to_processed_ms) AS avg_pulsar_to_processed_ms,

    MAX(CASE WHEN rn_processed_db = CEIL(0.99 * cnt) THEN processed_to_db_ms END) AS p99_processed_to_db_ms,
    MAX(CASE WHEN rn_processed_db = CEIL(0.95 * cnt) THEN processed_to_db_ms END) AS p95_processed_to_db_ms,
    MAX(CASE WHEN rn_processed_db = CEIL(0.90 * cnt) THEN processed_to_db_ms END) AS p90_processed_to_db_ms,
    MAX(CASE WHEN rn_processed_db = CEIL(0.50 * cnt) THEN processed_to_db_ms END) AS p50_processed_to_db_ms,
    AVG(processed_to_db_ms) AS avg_processed_to_db_ms
  FROM ranked;
END //

DELIMITER ;
