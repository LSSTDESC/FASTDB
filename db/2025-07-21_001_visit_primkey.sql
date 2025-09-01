-- Get rid of snapshot tables to simplify the rest.
-- Going to replace snapshots with something else
--   in a future migration.
DROP TABLE diaforcedsource_snapshot;
DROP TABLE diaobject_snapshot;
DROP TABLE diasource_snapshot;
DROP TABLE snapshot;

-- We have been told that diaobject will be unique across DRs.
--   (Some of the bits of the bigint encode which DR it is.)
--   So, there should be no need to use processing version in
--   diaobject foreign keys.  (We still will keep the
--   processing_version field in diaobject for simplicity of filtering.)
ALTER TABLE diaobject ADD CONSTRAINT unique_diaobjectid UNIQUE (diaobjectid);

-- Also, no need for the seprate table joining diaboject to
-- root diaboject, it can just be a single foreign key column
-- in diaobject.
DROP TABLE diaobject_root_map;
ALTER TABLE diaobject ADD COLUMN rootid uuid NOT NULL;
COMMENT ON COLUMN diaobject.rootid IS 'UUID of the root unique diaobject of this object';
ALTER TABLE diaobject ADD CONSTRAINT diaobject_root_fkey
  FOREIGN KEY (rootid) REFERENCES root_diaobject(id);

-- We have also been told that forced soruces will not be redone.
--   That is, once a given (diaObject,visit) is in the PDPB for
--   a forced source, that row will never change in the Project
--   PPDB, even if that object has updated forced photometry later.
--   (ROB ADD REFERENCE.)
DROP INDEX idx_diaforcedsource_diaobjectidpv;
ALTER TABLE diaforcedsource DROP CONSTRAINT diaforcedsource_pkey;
ALTER TABLE diaforcedsource DROP CONSTRAINT fk_diaforcedsource_diaobject;
ALTER TABLE diaforcedsource DROP COLUMN diaforcedsourceid;
ALTER TABLE diaforcedsource DROP COLUMN diaobject_procver;
ALTER TABLE diaforcedsource ADD PRIMARY KEY (processing_version,diaobjectid,visit);
ALTER TABLE diaforcedsource ADD CONSTRAINT fk_diaforcedsource_diaobjectid
  FOREIGN KEY (diaobjectid) REFERENCES diaobject(diaobjectid) ON DELETE CASCADE
  DEFERRABLE INITIALLY IMMEDIATE;
CREATE INDEX idx_diaforcedsource_diaobjectid ON diaforcedsource(diaobjectid);

DROP INDEX idx_diasource_diaobjectidpv;
ALTER TABLE diasource DROP CONSTRAINT fk_diasource_diaobject;
ALTER TABLE diasource DROP CONSTRAINT diasource_pkey;
ALTER TABLE diasource DROP COLUMN diasourceid;
ALTER TABLE diasource DROP COLUMN parentdiasourceid;
ALTER TABLE diasource DROP COLUMN diaobject_procver;
ALTER TABLE diasource ADD PRIMARY KEY (processing_version,diaobjectid,visit);
ALTER TABLE diasource ADD CONSTRAINT fk_diasource_diaobjectid
  FOREIGN KEY (diaobjectid) REFERENCES diaobject(diaobjectid) ON DELETE CASCADE
  DEFERRABLE INITIALLY IMMEDIATE;
CREATE INDEX idx_diasource_diaobjectid ON diasource(diaobjectid);

ALTER TABLE ppdb_diaforcedsource DROP CONSTRAINT ppdb_diaforcedsource_pkey;
ALTER TABLE ppdb_diaforcedsource DROP COLUMN diaforcedsourceid;
ALTER TABLE ppdb_diaforcedsource ADD PRIMARY KEY (diaobjectid,visit);

ALTER TABLE ppdb_alerts_sent DROP CONSTRAINT fk_ppdb_alerts_sent_diasource;
ALTER TABLE ppdb_alerts_sent DROP COLUMN diasourceid;

ALTER TABLE ppdb_diasource DROP CONSTRAINT ppdb_diasource_pkey;
ALTER TABLE ppdb_diasource DROP COLUMN diasourceid;
ALTER TABLE ppdb_diasource DROP COLUMN parentdiasourceid;
ALTER TABLE ppdb_diasource ADD PRIMARY KEY (diaobjectid,visit);

ALTER TABLE ppdb_alerts_sent ADD COLUMN diaobjectid bigint;
ALTER TABLE ppdb_alerts_sent ADD COLUMN visit bigint;
ALTER TABLE ppdb_alerts_sent ADD CONSTRAINT fk_ppdb_alerts_sent_ppdb_diasourfce
  FOREIGN KEY (diaobjectid,visit) REFERENCES ppdb_diasource(diaobjectid,visit);
