-- This one is a mess.
-- We are replacing processing_version with base_processing_version,
--   which is indexed by UUID rather than integer.
-- Then, we are adding processing_version, which points to
--   base_processing_version.
-- We have to edit every table that links to processing_version,
--   replacing that with a link to base_processing version, and trying
--   to do it in a way that maintains the integrity.

CREATE TABLE base_processing_version(
  id UUID NOT NULL DEFAULT gen_random_uuid(),
  description text NOT NULL,
  notes text DEFAULT NULL,
  created_at timestamp with time zone DEFAULT NOW()
);
ALTER TABLE base_processing_version ADD CONSTRAINT pk_base_procver PRIMARY KEY (id);
CREATE UNIQUE INDEX ix_base_procver_desc ON base_processing_Version USING btree (description);

ALTER TABLE processing_version RENAME TO old_processing_version;
ALTER TABLE processing_version_alias RENAME TO old_processing_version_alias;

CREATE TABLE processing_version(
  id UUID NOT NULL DEFAULT gen_random_uuid(),
  description text NOT NULL,
  notes text DEFAULT NULL,
  created_at timestamp with time zone NOT NULL DEFAULT NOW()
);
ALTER TABLE processing_version ADD CONSTRAINT pk_procver PRIMARY KEY (id);
CREATE UNIQUE INDEX ix_procver_description ON processing_version(description);

CREATE TABLE processing_version_alias(
  description text NOT NULL,
  procver_id UUID NOT NULL
);
ALTER TABLE processing_version_alias ADD CONSTRAINT pk_procver_alias PRIMARY KEY (description);
CREATE INDEX ix_procver_alias_id ON processing_version_alias(procver_id);
ALTER TABLE processing_version_alias ADD CONSTRAINT fk_procver_alias_procver
  FOREIGN KEY (procver_id) REFERENCES processing_Version(id) ON DELETE RESTRICT;

CREATE TABLE base_procver_of_procver(
  procver_id UUID NOT NULL,
  base_procver_id UUID NOT NULL,
  priority int NOT NULL
);
ALTER TABLE base_procver_of_procver ADD CONSTRAINT pk_base_procver_of_procver PRIMARY KEY (procver_id,base_procver_id);
CREATE UNIQUE INDEX ix_bpv_of_pv_bpv_prio ON base_procver_of_procver(procver_id,priority);
CREATE INDEX ix_bpv_of_pv_procver_id ON base_procver_of_procver(procver_id);
CREATE INDEX ix_bpv_of_pv_base_procver_id ON base_procver_of_procver(base_procver_id);
ALTER TABLE base_procver_of_procver ADD CONSTRAINT fk_bpv_of_pv_procver_id
  FOREIGN KEY (procver_id) REFERENCES processing_version(id) ON DELETE RESTRICT;
ALTER TABLE base_procver_of_procver ADD CONSTRAINT fk_bpv_of_pv_base_procver_id
  FOREIGN KEY (base_procver_id) REFERENCES base_processing_version(id) ON DELETE RESTRICT;



-- Because the previous database didn't have base processing versions,
--   for this one migration we can just make processing version a clone
--   of base processing version.
INSERT INTO base_processing_version(description) SELECT description FROM old_processing_version;
INSERT INTO processing_version(description) SELECT description FROM old_processing_version;
INSERT INTO base_procver_of_procver(procver_id,base_procver_id,priority)
  SELECT p.id,b.id,0
  FROM base_processing_version b
  INNER JOIN processing_version p ON b.description=p.description;

INSERT INTO processing_version_alias(description,procver_id)
  SELECT a.description,p.id FROM old_processing_version_alias a
  INNER JOIN old_processing_version opv ON a.id=opv.id
  INNER JOIN processing_version p ON opv.description=p.description;


ALTER TABLE diaforcedsource DROP CONSTRAINT diaforcedsource_pkey;
DROP INDEX idx_diaforcedsource_procver;
ALTER TABLE diaforcedsource DROP CONSTRAINT fk_diaforcedsource_procver;
ALTER TABLE diaforcedsource RENAME COLUMN processing_version to old_procver;
ALTER TABLE diaforcedsource ADD COLUMN base_procver_id UUID;
UPDATE diaforcedsource SET base_procver_id=subq.id
  FROM ( SELECT b.id,opv.id AS oldid FROM base_processing_version b
         INNER JOIN old_processing_version opv ON opv.description=b.description
       )  subq
  WHERE subq.oldid=diaforcedsource.old_procver;
ALTER TABLE diaforcedsource ALTER COLUMN base_procver_id SET NOT NULL;
ALTER TABLE diaforcedsource ADD PRIMARY KEY (base_procver_id, diaobjectid, visit);
ALTER TABLE diaforcedsource ADD CONSTRAINT fk_diaforcedsource_procver
  FOREIGN KEY(base_procver_id) REFERENCES base_processing_version(id) ON DELETE RESTRICT;
CREATE INDEX idx_diaforcedsource_procver ON diaforcedsource(base_procver_id);
ALTER TABLE diaforcedsource DROP COLUMN old_procver;
 

ALTER TABLE diaobject DROP CONSTRAINT diaobject_pkey;
DROP INDEX idx_diaobject_procver;
ALTER TABLE diaobject DROP CONSTRAINT fk_diaobject_procver;
ALTER TABLE diaobject RENAME COLUMN processing_version to old_procver;
ALTER TABLE diaobject ADD COLUMN base_procver_id UUID;
UPDATE diaobject SET base_procver_id=subq.id
FROM ( SELECT b.id,opv.id AS oldid FROM base_processing_version b
       INNER JOIN old_processing_version opv ON opv.description=b.description
     )  subq
WHERE subq.oldid=diaobject.old_procver;
ALTER TABLE diaobject ALTER COLUMN base_procver_id SET NOT NULL;
ALTER TABLE diaobject ADD PRIMARY KEY (diaobjectid, base_procver_id);
ALTER TABLE diaobject ADD CONSTRAINT fk_diaobject_procver
  FOREIGN KEY(base_procver_id) REFERENCES base_processing_version(id) ON DELETE RESTRICT;
CREATE INDEX idx_diaobject_procver ON diaobject(base_procver_id);
ALTER TABLE diaobject DROP COLUMN old_procver;
 

ALTER TABLE diasource DROP CONSTRAINT diasource_pkey;
DROP INDEX idx_diasource_procver;
ALTER TABLE diasource DROP CONSTRAINT fk_diasource_procver;
ALTER TABLE diasource RENAME COLUMN processing_version to old_procver;
ALTER TABLE diasource ADD COLUMN base_procver_id UUID;
UPDATE diasource SET base_procver_id=subq.id
  FROM ( SELECT b.id,opv.id AS oldid FROM base_processing_version b
         INNER JOIN old_processing_version opv ON opv.description=b.description
       )  subq
  WHERE subq.oldid=diasource.old_procver;
ALTER TABLE diasource ALTER COLUMN base_procver_id SET NOT NULL;
ALTER TABLE diasource ADD PRIMARY KEY (base_procver_id, diaobjectid, visit);
ALTER TABLE diasource ADD CONSTRAINT fk_diasource_procver
  FOREIGN KEY(base_procver_id) REFERENCES base_processing_version(id) ON DELETE RESTRICT;
CREATE INDEX idx_diasource_procver ON diasource(base_procver_id);
ALTER TABLE diasource DROP COLUMN old_procver;
 

DROP INDEX idx_hostgalaxy_procver;
ALTER TABLE host_galaxy RENAME COLUMN processing_version to old_procver;
ALTER TABLE host_galaxy ADD COLUMN base_procver_id UUID;
UPDATE host_galaxy SET base_procver_id=subq.id
FROM ( SELECT b.id,opv.id AS oldid FROM base_processing_version b
       INNER JOIN old_processing_version opv ON opv.description=b.description
     )  subq
WHERE subq.oldid=host_galaxy.old_procver;
ALTER TABLE host_galaxy ALTER COLUMN base_procver_id SET NOT NULL;
ALTER TABLE host_galaxy ADD CONSTRAINT fk_hostgalaxy_procver
  FOREIGN KEY(base_procver_id) REFERENCES base_processing_version(id) ON DELETE RESTRICT;
CREATE INDEX idx_hostgalaxy_procver ON host_galaxy(base_procver_id);
ALTER TABLE host_galaxy DROP COLUMN old_procver;
 

DROP TABLE old_processing_version_alias;
DROP TABLE old_processing_version;
