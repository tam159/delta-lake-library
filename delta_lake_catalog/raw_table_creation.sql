CREATE TABLE IF NOT EXISTS raw.transaction (
    topic STRING,
    partition INT,
    offset BIGINT,
    timestamp TIMESTAMP,
    timestampType INT,
    clientId STRING NOT NULL,
    clientTransactionId STRING,
    clientCustomerId STRING,
    clientPaymentSourceId STRING,
    data STRING)
USING DELTA
PARTITIONED BY (clientId)
LOCATION '/mnt/raw/transaction'
TBLPROPERTIES (
    delta.enableChangeDataFeed = true,
    delta.autoOptimize.optimizeWrite = true,
    delta.autoOptimize.autoCompact = true);

CREATE TABLE IF NOT EXISTS raw.cdc_client (
    key STRING,
    value STRING,
    topic STRING,
    partition INT,
    offset BIGINT,
    timestamp TIMESTAMP,
    timestampType INT,
    schema STRING,
    after STRING,
    patch STRING,
    filter STRING,
    source STRING,
    op STRING,
    ts_ms BIGINT,
    transaction STRING)
USING DELTA
LOCATION '/mnt/raw/cdc_client'
TBLPROPERTIES (
    delta.enableChangeDataFeed = true,
    delta.autoOptimize.optimizeWrite = true,
    delta.autoOptimize.autoCompact = true);
