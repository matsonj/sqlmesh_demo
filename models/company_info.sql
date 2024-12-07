MODEL (
  name raw.companies,
  kind FULL
);

set variable my_list = (
    select array_agg(file) from raw.files where entity = 'ticker_info'
  );

SELECT
  info.symbol || '-' || info.filename AS id,
  info.*,
  files.modified_ts,
  NOW() AT TIME ZONE 'UTC' AS updated_ts
FROM READ_CSV(GETVARIABLE('my_list'), filename = TRUE, union_by_name = TRUE) AS info
LEFT JOIN raw.files
