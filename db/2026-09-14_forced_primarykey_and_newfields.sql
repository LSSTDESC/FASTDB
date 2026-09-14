-- edp2 doesn't have a diaforcedsourceid, so we can't use that in a primary key.

ALTER TABLE diaforcedsource_extra DROP CONSTRAINT fk_diaforcedsource_extra_diaforcedsource;
ALTER TABLE diaforcedsource DROP CONSTRAINT diaforcedsource_pkey;
ALTER TABLE diaforcedsource_extra DROP CONSTRAINT diaforcedsource_extra_pkey;

ALTER TABLE diaforcedsource_extra ADD COLUMN diaobjectid bigint;
ALTER TABLE diaforcedsource_extra ADD COLUMN visit bigint;
-- This next migration line will work with existing fastdbs because they
--    all have a diaforcedsource as a primary key.  It won't
--    work... well, it won't work after this migration is applied, so I
--    guess that's nothing to worry about.
UPDATE diaforcedsource_extra e SET diaobjectid=f.diaobjectid, visit=f.visit
  FROM diaforcedsource f
  WHERE f.diaforcedsourceid=e.diaforcedsourceid;

ALTER TABLE diaforcedsource_extra ALTER COLUMN diaobjectid SET NOT NULL;
ALTER TABLE diaforcedsource_extra ALTER COLUMN visit SET NOT NULL;

ALTER TABLE diaforcedsource ADD CONSTRAINT diaforcedsource_pkey
  PRIMARY KEY (base_procver_id, diaobjectid, visit);

ALTER TABLE diaforcedsource_extra ADD CONSTRAINT diaforcedsource_extra_pkey
  PRIMARY KEY (base_procver_id, diaobjectid, visit);
ALTER TABLE diaforcedsource_extra ADD CONSTRAINT fk_diaforcedsource_extra_diaforcedsource
  FOREIGN KEY (base_procver_id, diaobjectid, visit) REFERENCES diaforcedsource(base_procver_id, diaobjectid, visit)
  ON DELETE CASCADE;
