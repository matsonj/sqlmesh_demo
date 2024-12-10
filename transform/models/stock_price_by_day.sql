MODEL (
  name mart.stock_price_by_day,
  kind VIEW,
  grain (trade_date, stock_symbol)
);

SELECT
  c.symbol AS stock_symbol,
  c.shares_outstanding,
  sp.close,
  sp.trade_date,
  ROUND(c.shares_outstanding::REAL * sp.close::REAL, 0) AS market_cap
FROM interim.stock_info AS c
LEFT JOIN interim.stock_history AS sp
  ON c.symbol = sp.symbol
ORDER BY
  c.symbol,
  sp.trade_date