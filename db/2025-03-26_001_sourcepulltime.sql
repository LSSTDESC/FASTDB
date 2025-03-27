-- This table holds the last time that we imported
--   sources from mongo into postgres for each collection.

CREATE TABLE diasource_import_time(
   collection text PRIMARY KEY,
   t timestamp with time zone
);
