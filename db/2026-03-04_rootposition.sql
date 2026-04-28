ALTER TABLE root_diaobject ADD COLUMN ra double precision;
ALTER TABLE root_diaobject ADD COLUMN dec double precision;

-- Going to select a random diaobject to pull the position from
UPDATE root_diaobject r SET ra=q.ra, dec=q.dec
FROM (
  SELECT DISTINCT ON(o.rootid) o.rootid, p.ra, p.dec
  FROM diaobject o
  INNER JOIN diaobject_position p ON o.diaobjectid=p.diaobjectid
  ORDER BY o.rootid
) q
WHERE q.rootid=r.id;

CREATE INDEX ix_rootdiaobject_q3c ON root_diaobject( q3c_ang2ipix(ra, dec) );
