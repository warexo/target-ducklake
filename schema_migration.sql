-- migration sql for ducklake 0.1 to 0.2
-- for issue https://github.com/duckdb/ducklake/issues/240

ALTER TABLE public.ducklake_schema
  ADD COLUMN path TEXT DEFAULT '',
  ADD COLUMN path_is_relative BOOLEAN DEFAULT TRUE;

ALTER TABLE public.ducklake_table
  ADD COLUMN path TEXT DEFAULT '',
  ADD COLUMN path_is_relative BOOLEAN DEFAULT TRUE;

ALTER TABLE public.ducklake_metadata
  ADD COLUMN scope TEXT,
  ADD COLUMN scope_id BIGINT;

ALTER TABLE public.ducklake_data_file
  ADD COLUMN mapping_id BIGINT;

CREATE TABLE public.ducklake_column_mapping (
  mapping_id    BIGINT,
  table_id      BIGINT,
  "type"        TEXT
);

CREATE TABLE public.ducklake_name_mapping (
  mapping_id       BIGINT,
  column_id        BIGINT,
  source_name      TEXT,
  target_field_id  BIGINT,
  parent_column    BIGINT
);

UPDATE public.ducklake_partition_column AS pc
SET column_id = sub.a[pc.column_id + 1]
FROM (
  SELECT
    table_id,
    array_agg(column_id ORDER BY column_order) AS a
  FROM public.ducklake_column
  WHERE parent_column IS NULL
    AND end_snapshot  IS NULL
  GROUP BY table_id
) AS sub
WHERE sub.table_id = pc.table_id;

UPDATE public.ducklake_metadata
SET value = '0.2'
WHERE "key" = 'version';