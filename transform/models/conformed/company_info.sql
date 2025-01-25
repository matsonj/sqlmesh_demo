MODEL (
  name conformed.company_info,
  kind FULL,
  grain (
    symbol
  ),
  audits (UNIQUE_VALUES(columns = (
      symbol
    )), NOT_NULL(columns = (
      symbol
  ))),
  cron '@daily',
  -- enabled false
);

SELECT
  *
FROM interim.stock_info
WHERE
  valid_to IS NULL