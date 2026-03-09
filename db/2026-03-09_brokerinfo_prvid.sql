ALTER TABLE diasource_brokerinfo ADD COLUMN diaobjectid bigint;
UPDATE diasource_brokerinfo b SET diaobjectid=subq.diaobjectid
  FROM ( SELECT diasourceid, diaobjectid FROM diasource ) AS subq
  WHERE subq.diasourceid=b.diasourceid;
ALTER TABLE diasource_brokerinfo ALTER COLUMN diaobjectid SET NOT NULL;
ALTER TABLE diasource_brokerinfo ADD COLUMN prv_diasourceid bigint[];
ALTER TABLE diasource_brokerinfo ADD COLUMN prv_diaforcedsourceid bigint[];
