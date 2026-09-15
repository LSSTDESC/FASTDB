-- Should have done this in the previous migration, oh well, didn't need it until now
ALTER TABLE diaforcedsource ALTER COLUMN diaforcedsourceid DROP NOT NULL;
ALTER TABLE diaforcedsource_extra ALTER COLUMN diaforcedsourceid DROP NOT NULL;

-- Columns that were in edp2 that we didn't have previously
ALTER TABLE diasource_extra ADD COLUMN psfmag real DEFAULT NULL;
ALTER TABLE diasource_extra ADD COLUMN psfmagerr real DEFAULT NULL;
ALTER TABLE diasource_extra ADD COLUMN sciencemag real DEFAULT NULL;
ALTER TABLE diasource_extra ADD COLUMN sciencemagerr real DEFAULT NULL;

ALTER TABLE diaforcedsource_extra ADD COLUMN coord_ra real DEFAULT NULL;
ALTER TABLE diaforcedsource_extra ADD COLUMN coord_dec real DEFAULT NULL;
ALTER TABLE diaforcedsource_extra ADD COLUMN psfdiffflux real DEFAULT NULL;
ALTER TABLE diaforcedsource_extra ADD COLUMN psfdifffluxerr real DEFAULT NULL;
ALTER TABLE diaforcedsource_extra ADD COLUMN psfmag real DEFAULT NULL;
ALTER TABLE diaforcedsource_extra ADD COLUMN psfmagerr real DEFAULT NULL;
ALTER TABLE diaforcedsource_extra ADD COLUMN flags integer DEFAULT NULL;
ALTER TABLE diaforcedsource_extra ADD COLUMN pixelflags integer DEFAULT NULL;
