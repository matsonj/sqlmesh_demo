MODEL (
  name mart_stock.incremental_stock_price_by_day,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column close_date
  ),
  grain (close_date, stock_symbol)
);

SELECT
  c.symbol AS stock_symbol,
  c.shares_outstanding,
  sp.close,
  sp.date AS close_date,
  ROUND(c.shares_outstanding::REAL * sp.close::REAL, 0) AS market_cap
FROM stock_data_sqlmesh.incremental_stock_info AS c
LEFT JOIN stock_data_sqlmesh.incremental_stock_history AS sp
  ON c.symbol = sp.symbol
ORDER BY
  c.symbol,
  sp.date