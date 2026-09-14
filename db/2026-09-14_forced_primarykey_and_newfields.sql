-- edp2 doesn't have a diaforcedsourceid, so we can't use that in a primary key.
--
-- I don't want to make diaobjectid part of the primary key, because empirically in the
--    alert stream, the same physical object has more than one diaobjectid, and at different
--    times the same diaforcedsourceid is associated with different diaobjectids.
--    I want to avoid having duplicate diaforcedsource entries in fastdb; because of this
--    duplication in the alert stream, keying on diaobjectid won't accomplish that.
-- rootid is a bit scary because it's my own deduplication, but, well, here we go.
--
-- We're going to leave the diabobjectid column in diaforcedsource, but its meaning becomes
--    "one of the diaobjectids that was associated with this diaforcedsource at one point
--    or another".  It's not a complete record of all the diaobjectids that got associated
--    with a given diaforcedsource.

ALTER TABLE diaforcedsource_extra DROP CONSTRAINT fk_diaforcedsource_extra_diaforcedsource;
ALTER TABLE diaforcedsource DROP CONSTRAINT diaforcedsource_pkey;
ALTER TABLE diaforcedsource_extra DROP CONSTRAINT diaforcedsource_extra_pkey;

ALTER TABLE diaforcedsource ADD COLUMN rootid uuid;
UPDATE diaforcedsource f SET rootid=o.rootid FROM diaobject o WHERE f.diaobjectid=o.diaobjectid;

ALTER TABLE diaforcedsource_extra ADD COLUMN rootid uuid;
ALTER TABLE diaforcedsource_extra ADD COLUMN visit bigint;
-- This next column is redundant; you can get it from the
--    link back to diaforcedsource.  However, we need it for
--    use during our imports.  So annoying that there's no
--    diaforcedsourceid in edp2.
ALTER TABLE diaforcedsource_extra ADD COLUMN diaobjectid bigint;
-- This next migration line will work with existing fastdbs because they
--    all have a diaforcedsource as a primary key.  It won't
--    work... well, it won't work after this migration is applied, so I
--    guess that's nothing to worry about.
UPDATE diaforcedsource_extra e SET rootid=f.rootid, visit=f.visit, diaobjectid=f.diaobjectid
  FROM diaforcedsource f
  WHERE f.diaforcedsourceid=e.diaforcedsourceid;

ALTER TABLE diaforcedsource ALTER COLUMN diaobjectid DROP NOT NULL;
ALTER TABLE diaforcedsource ALTER COLUMN rootid SET NOT NULL;
ALTER TABLE diaforcedsource_extra ALTER COLUMN rootid SET NOT NULL;
ALTER TABLE diaforcedsource_extra ALTER COLUMN visit SET NOT NULL;

ALTER TABLE diaforcedsource ADD CONSTRAINT diaforcedsource_pkey
  PRIMARY KEY (base_procver_id, rootid, visit);
CREATE INDEX idx_diaforcedsource_rootid ON diaforcedsource(rootid);
ALTER TABLE diaforcedsource ADD CONSTRAINT fk_diaforcedsource_rootid
  FOREIGN KEY (rootid) REFERENCES root_diaobject(id);

ALTER TABLE diaforcedsource_extra ADD CONSTRAINT diaforcedsource_extra_pkey
  PRIMARY KEY (base_procver_id, rootid, visit);
ALTER TABLE diaforcedsource_extra ADD CONSTRAINT fk_diaforcedsource_extra_diaforcedsource
  FOREIGN KEY (base_procver_id, rootid, visit) REFERENCES diaforcedsource(base_procver_id, rootid, visit);
