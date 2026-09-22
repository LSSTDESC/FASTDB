ALTER TABLE diaobject ADD COLUMN procver_id uuid;
ALTER TABLE diaobject_position ADD COLUMN procver_id uuid;
ALTER TABLE diasource ADD COLUMN procver_id uuid;
ALTER TABLE diaforcedsource ADD COLUMN procver_id uuid;

UPDATE diaobject o SET procver_id=q.procver_id
FROM (
 SELECT o.diaobjectid, j.procver_id
 FROM diaobject o
 INNER JOIN base_procver_of_procver j ON o.base_procver_id=j.base_procver_id
) q
WHERE q.diaobjectid=o.diaobjectid;

UPDATE diaobject_position p SET procver_id=q.procver_id
FROM (
 SELECT p.diaobjectid, j.procver_id
 FROM diaobject_position p
 INNER JOIN base_procver_of_procver j ON p.base_procver_id=j.base_procver_id
) q
WHERE q.diaobjectid=o.diaobjectid;

UPDATE diasource s SET procver_id=q.procver_id
FROM (
 SELECT s.diasourceid, j.procver_id
 FROM diasource s
 INNER JOIN base_procver_of_procver j ON s.base_procver_id=j.base_procver_id
) q
WHERE q.diasourceid=o.diasourceid;

UPDATE diaforcedsource f SET procver_id=q.procver_id
FROM (
 SELECT f.diaforcedsourceid j.procver_id
 FROM diaforcedsource f
 INNER JOIN base_procver_of_procver j ON f.base_procver_id=j.base_procver_id
) q
WHERE q.diaforcedsourceid=o.diaforcedsourceid;

ALTER INDEX idx_diaobject_base_procver_id RENAME TO idx_diaobject_position_base_procver_id;
ALTER INDEX idx_diaobject_procver RENAME TO idx_diaobject_base_procver_id;
CREATE INDEX idx_diaobject_procver_id ON diaobject (procver_id);
CREATE INDEX idx_diasource_procver_id ON diasource (procver_id);
CREATE INDEX idx_diaforcedsource_procver_id ON diaforcedsource (procver_id);
CREATE INDEX Idx_diaobject_position_procver_id ON diaobject_position (procver_id);
