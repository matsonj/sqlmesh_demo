MODEL (
  name stock_data_sqlmesh.incremental_stock_options,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column _dlt_load_time,
  ),
  grain (contract_symbol, strike, type, expiration, symbol),
);

SELECT
  CAST(contract_symbol AS TEXT) AS contract_symbol,
  CAST(last_trade_date AS TIMESTAMP) AS last_trade_date,
  CAST(strike AS DOUBLE) AS strike,
  CAST(last_price AS DOUBLE) AS last_price,
  CAST(bid AS DOUBLE) AS bid,
  CAST(ask AS DOUBLE) AS ask,
  CAST(change AS DOUBLE) AS change,
  CAST(percent_change AS DOUBLE) AS percent_change,
  CAST(open_interest AS BIGINT) AS open_interest,
  CAST(implied_volatility AS DOUBLE) AS implied_volatility,
  CAST(in_the_money AS BOOLEAN) AS in_the_money,
  CAST(contract_size AS TEXT) AS contract_size,
  CAST(currency AS TEXT) AS currency,
  CAST(type AS TEXT) AS type,
  CAST(expiration AS TEXT) AS expiration,
  CAST(symbol AS TEXT) AS symbol,
  CAST(_dlt_load_id AS TEXT) AS _dlt_load_id,
  CAST(_dlt_id AS TEXT) AS _dlt_id,
  CAST(volume AS DOUBLE) AS volume,
  TO_TIMESTAMP(CAST(_dlt_load_id AS DOUBLE)) as _dlt_load_time
FROM
  stock_data.stock_options
WHERE
  TO_TIMESTAMP(CAST(_dlt_load_id AS DOUBLE)) BETWEEN @start_ds AND @end_ds
