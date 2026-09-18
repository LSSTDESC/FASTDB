-- Yeah, so, some of the columns I added for edp2, we aren't actually saving anyway
--   (see load_edp2_parquet.py).  Yank those columns.  (Some of these are not
--   documented on https://sdm-schemas.lsst.io/v/EDP2-deploy-v3/dp2.html ; in
--   diaforcedsource_extra, edp2 calls psfdiffflux what alerts call psfflux, which
--   is extremely annoying.)
--
-- I predict we decide to add them again later and redo edp2 loading... but, whatevs.

ALTER TABLE diasource_extra DROP COLUMN psfmag;
ALTER TABLE diasource_extra DROP COLUMN psfmagerr;
ALTER TABLE diasource_extra DROP COLUMN sciencemag;
ALTER TABLE diasource_extra DROP COLUMN sciencemagerr;

ALTER TABLE diaforcedsource_extra DROP COLUMN coord_ra;
ALTER TABLE diaforcedsource_extra DROP COLUMN coord_dec;
ALTER TABLE diaforcedsource_extra DROP COLUMN psfdiffflux;
ALTER TABLE diaforcedsource_extra DROP COLUMN psfdifffluxerr;
ALTER TABLE diaforcedsource_extra DROP COLUMN psfmag;
ALTER TABLE diaforcedsource_extra DROP COLUMN psfmagerr;
