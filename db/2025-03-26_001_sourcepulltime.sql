-- This table is a singlet that holds the last time that we imported
--   sources from mongo into postgres.

CREATE TABLE diasource_import_time(
   t timestamp with time zone
);
