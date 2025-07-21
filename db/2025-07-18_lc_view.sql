-- View joining diaobject with all matching diasource and diaforcedsource rows
-- Produces one diaobject row with JSONB arrays of diasource and diaforcedsource structs
CREATE OR REPLACE VIEW diaobject_with_all_sources AS
SELECT
  d.*,
  COALESCE(ds.diasources, '[]')     AS diasources,
  COALESCE(fs.forced_sources, '[]') AS forced_sources
FROM diaobject d
LEFT JOIN (
  SELECT
    diaobjectid,
    diaobject_procver,
    processing_version,
    jsonb_agg(to_jsonb(s) ORDER BY s.midpointmjdtai) AS diasources
  FROM diasource s
  WHERE processing_version = diaobject_procver
  GROUP BY diaobjectid, diaobject_procver, processing_version
) ds
  ON d.diaobjectid = ds.diaobjectid
  AND d.processing_version = ds.diaobject_procver
  AND d.processing_version = ds.processing_version
LEFT JOIN (
  SELECT
    diaobjectid,
    diaobject_procver,
    processing_version,
    jsonb_agg(to_jsonb(fs) ORDER BY fs.midpointmjdtai) AS forced_sources
  FROM diaforcedsource fs
  WHERE processing_version = diaobject_procver
  GROUP BY diaobjectid, diaobject_procver, processing_version
) fs
  ON d.diaobjectid = fs.diaobjectid
  AND d.processing_version = fs.diaobject_procver
  AND d.processing_version = fs.processing_version;
