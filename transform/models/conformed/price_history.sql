MODEL (
  name conformed.price_history,
  kind VIEW,
  cron '@daily',
  -- enabled false
);

SELECT
  *
FROM interim.stock_history