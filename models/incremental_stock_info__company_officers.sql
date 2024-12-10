MODEL (
  name stock_data_sqlmesh.incremental_stock_info__company_officers,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column _dlt_load_time
  )
);

SELECT
  CO.max_age::BIGINT AS max_age,
  CO.name::TEXT AS name,
  CO.age::BIGINT AS age,
  CO.title::TEXT AS title,
  CO.year_born::BIGINT AS year_born,
  CO.fiscal_year::BIGINT AS fiscal_year,
  CO.total_pay::BIGINT AS total_pay,
  CO.exercised_value::BIGINT AS exercised_value,
  CO.unexercised_value::BIGINT AS unexercised_value,
  CO._dlt_parent_id::TEXT AS _dlt_parent_id,
  CO._dlt_list_idx::BIGINT AS _dlt_list_idx,
  CO._dlt_id::TEXT AS _dlt_id,
  TO_TIMESTAMP(SI._dlt_load_id::DOUBLE) AS _dlt_load_time
FROM stock_data.stock_info__company_officers AS CO
LEFT JOIN stock_data.stock_info AS SI
  ON CO._dlt_parent_id = SI._dlt_id
WHERE
  TO_TIMESTAMP(_dlt_load_id::DOUBLE) BETWEEN @start_ds AND @end_ds