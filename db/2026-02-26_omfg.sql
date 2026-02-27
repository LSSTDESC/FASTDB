-- THIS WILL NOT WORK ON A PRODUCTION DATABASE
-- It will only work if the database is empty, otherwise diaforcedsourceid does not get populated.
-- As of this writing, I was willing to wipe out all the databases I had.
--   (In any event, it was an unsolvable problem, because in the alert stream diaobjectid was not
--    unnique, so it would have been impossible to find the right diaforcedsoruceid anyway.)

-- It turns out, finally seeing the LSST alert stream, that diaobjectid is not reliable
-- the same diasourceid shows up in subsequent alerts from LSST with a *different* diaobjectid sometimes
-- cannot use it as a foreign key from diasource to diasource_extra, etc.

ALTER TABLE diaforcedsource_extra DROP CONSTRAINT fk_diaforcedsource_extra_diaforcedsource;
ALTER TABLE diaforcedsource_extra DROP CONSTRAINT diaforcedsource_extra_pkey;
ALTER TABLE diaforcedsource_extra DROP COLUMN diaobjectid;
ALTER TABLE diaforcedsource_extra DROP COLUMN visit;

ALTER TABLE diaforcedsource DROP CONSTRAINT pk_diaforcedsource;

DROP INDEX idx_diasource_brokerinfo_sourcespec;
ALTER TABLE diasource_brokerinfo DROP CONSTRAINT fk_diasource_brokerinfo_diasource;
ALTER TABLE diasource_brokerinfo DROP COLUMN diaobjectid;
ALTER TABLE diasource_brokerinfo DROP COLUMN visit;

ALTER TABLE diaforcedsource ADD PRIMARY KEY (diaforcedsourceid, base_procver_id);

ALTER TABLE diaforcedsource_extra ADD COLUMN diaforcedsourceid bigint;
ALTER TABLE diaforcedsource_extra ADD PRIMARY KEY ( diaforcedsourceid, base_procver_id );
ALTER TABLE diaforcedsource_extra ADD CONSTRAINT fk_diaforcedsource_extra_diaforcedsource
  FOREIGN KEY (diaforcedsourceid, base_procver_id)
  REFERENCES diaforcedsource(diaforcedsourceid, base_procver_id)
  ON DELETE CASCADE
  DEFERRABLE INITIALLY IMMEDIATE;

CREATE INDEX idx_diasource_brokerinfo_sourcespec ON diasource_brokerinfo(diasourceid, base_procver_id);
ALTER TABLE diasource_brokerinfo ADD CONSTRAINT fk_diasource_brokerinfo_diasource
  FOREIGN KEY (diasourceid, base_procver_id)
  REFERENCES diasource(diasourceid, base_procver_id)
  ON DELETE CASCADE
  DEFERRABLE INITIALLY IMMEDIATE;
