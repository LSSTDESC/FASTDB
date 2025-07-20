CREATE EXTENSION IF NOT EXISTS pg_healpix;

ALTER TABLE diaobject ADD COLUMN npix_order29 BIGINT;

UPDATE diaobject SET npix_order29 = healpix_ang2ipix_nest(8192, ra, dec);

CREATE INDEX idx_diaobject_npix29 ON diaobject USING btree(npix_order29);
