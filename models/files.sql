MODEL (
  name raw.files,
  cron '@daily',
  grain file
);

SELECT
  "filename" AS "file",
  REGEXP_EXTRACT("filename", 'data/(.+?)_\d+\.csv', 1) AS entity,
  last_modified AS modified_ts
FROM READ_BLOB('data/*.csv')