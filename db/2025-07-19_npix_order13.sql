CREATE EXTENSION IF NOT EXISTS pg_healpix;

ALTER TABLE diaobject ADD COLUMN npix_order13 BIGINT;

UPDATE diaobject SET npix_order13 = healpix_ang2ipix_nest(8192, ra, dec);

CREATE INDEX idx_diaobject_npix13 ON diaobject USING btree(npix_order13);
