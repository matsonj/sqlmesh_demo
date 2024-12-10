MODEL (
  name stock_data_sqlmesh.incremental_stock_info__company_officers,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column _dlt_load_time,
  ),
);

SELECT
  CAST(CO.max_age AS BIGINT) AS max_age,
  CAST(CO.name AS TEXT) AS name,
  CAST(CO.age AS BIGINT) AS age,
  CAST(CO.title AS TEXT) AS title,
  CAST(CO.year_born AS BIGINT) AS year_born,
  CAST(CO.fiscal_year AS BIGINT) AS fiscal_year,
  CAST(CO.total_pay AS BIGINT) AS total_pay,
  CAST(CO.exercised_value AS BIGINT) AS exercised_value,
  CAST(CO.unexercised_value AS BIGINT) AS unexercised_value,
  CAST(CO._dlt_parent_id AS TEXT) AS _dlt_parent_id,
  CAST(CO._dlt_list_idx AS BIGINT) AS _dlt_list_idx,
  CAST(CO._dlt_id AS TEXT) AS _dlt_id, 
  TO_TIMESTAMP(CAST(SI._dlt_load_id AS DOUBLE)) as _dlt_load_time
FROM
  stock_data.stock_info__company_officers CO
  LEFT JOIN stock_data.stock_info SI
    ON CO._dlt_parent_id = SI._dlt_id
WHERE
  TO_TIMESTAMP(CAST(_dlt_load_id AS DOUBLE)) BETWEEN @start_ds AND @end_ds
