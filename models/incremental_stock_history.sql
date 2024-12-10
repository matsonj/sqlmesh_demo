MODEL (
  name stock_data_sqlmesh.incremental_stock_history,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column _dlt_load_time
  ),
  grain (date, symbol)
);

SELECT
  date::TEXT AS date,
  open::DOUBLE AS open,
  high::DOUBLE AS high,
  low::DOUBLE AS low,
  close::DOUBLE AS close,
  adj_close::DOUBLE AS adj_close,
  volume::BIGINT AS volume,
  symbol::TEXT AS symbol,
  _dlt_load_id::TEXT AS _dlt_load_id,
  _dlt_id::TEXT AS _dlt_id,
  TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS _dlt_load_time
FROM stock_data.stock_history
WHERE
  TO_TIMESTAMP(_dlt_load_id::DOUBLE) BETWEEN @start_ds AND @end_ds