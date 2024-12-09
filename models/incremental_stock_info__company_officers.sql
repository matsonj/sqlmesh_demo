MODEL (
  name stock_data_sqlmesh.incremental_stock_info__company_officers,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column _dlt_load_time,
  ),
);

SELECT
  CAST(max_age AS BIGINT) AS max_age,
  CAST(name AS TEXT) AS name,
  CAST(age AS BIGINT) AS age,
  CAST(title AS TEXT) AS title,
  CAST(year_born AS BIGINT) AS year_born,
  CAST(fiscal_year AS BIGINT) AS fiscal_year,
  CAST(total_pay AS BIGINT) AS total_pay,
  CAST(exercised_value AS BIGINT) AS exercised_value,
  CAST(unexercised_value AS BIGINT) AS unexercised_value,
  CAST(_dlt_parent_id AS TEXT) AS _dlt_parent_id,
  CAST(_dlt_list_idx AS BIGINT) AS _dlt_list_idx,
  CAST(_dlt_id AS TEXT) AS _dlt_id,
  TO_TIMESTAMP(CAST(_dlt_load_id AS DOUBLE)) as _dlt_load_time
FROM
  stock_data.stock_info__company_officers
WHERE
  TO_TIMESTAMP(CAST(_dlt_load_id AS DOUBLE)) BETWEEN @start_ds AND @end_ds
