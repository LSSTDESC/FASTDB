ALTER TABLE diasource ALTER COLUMN visit TYPE bigint,
                      ALTER COLUMN visit SET NOT NULL;
ALTER TABLE diaforcedsource ALTER COLUMN visit TYPE bigint,
                            ALTER COLUMN visit SET NOT NULL;
ALTER TABLE ppdb_diasource ALTER COLUMN visit TYPE bigint,
                           ALTER COLUMN visit SET NOT NULL;
ALTER TABLE ppdb_diaforcedsource ALTER COLUMN visit TYPE bigint,
                                 ALTER COLUMN visit SET NOT NULL;
