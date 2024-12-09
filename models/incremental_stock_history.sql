MODEL (
  name stock_data_sqlmesh.incremental_stock_history,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column _dlt_load_time,
  ),
  grain (date, symbol),
);

SELECT
  CAST(date AS TEXT) AS date,
  CAST(open AS DOUBLE) AS open,
  CAST(high AS DOUBLE) AS high,
  CAST(low AS DOUBLE) AS low,
  CAST(close AS DOUBLE) AS close,
  CAST(adj_close AS DOUBLE) AS adj_close,
  CAST(volume AS BIGINT) AS volume,
  CAST(symbol AS TEXT) AS symbol,
  CAST(_dlt_load_id AS TEXT) AS _dlt_load_id,
  CAST(_dlt_id AS TEXT) AS _dlt_id,
  TO_TIMESTAMP(CAST(_dlt_load_id AS DOUBLE)) as _dlt_load_time
FROM
  stock_data.stock_history
WHERE
  TO_TIMESTAMP(CAST(_dlt_load_id AS DOUBLE)) BETWEEN @start_ds AND @end_ds
