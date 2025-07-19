-- View joining diaobject with all matching diasource rows
-- Produces one diaobject row with a JSONB array of diasource structs
CREATE OR REPLACE VIEW diaobject_with_sources AS
SELECT d.*,
       COALESCE(
           (
               SELECT jsonb_agg(to_jsonb(s) ORDER BY s.midpointmjdtai)
               FROM diasource s
               WHERE s.diaobjectid = d.diaobjectid
                 AND s.diaobject_procver = d.processing_version
           ),
           '[]'::jsonb
       ) AS diasources
FROM diaobject d;