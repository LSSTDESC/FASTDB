-- Update table schema based on the lsst v9.0 alert schema
-- It's not an exhaustive copy, but....
-- Note that the current version of alerts have NO host
--   association in diaObject.  Leave those columns in for now,
--   however, in ancitipation that we'll eventually have it
--   (if only with DRs).

-- We are not going to add the lastsourcemjdtai, etc.  diaobject
--   columns, because we are hoping we won't have to deal with updating
--   the diaobject table.  This means the ra/dec will never get updated.
--   For DR, that doesn't matter.  For realtime, we will just have to
--   hope the very first ra/dec is close enough for the searches we do.
--   To actually find positions, we should look at diasources.
ALTER TABLE diaobject DROP COLUMN validitystart;
ALTER TABLE diaobject DROP COLUMN validityend;
ALTER TABLE diaobject DROP COLUMN radecmjdtai;
ALTER TABLE diaobject DROP COLUMN parallax;
ALTER TABLE diaobject DROP COLUMN parallaxerr;
ALTER TABLE diaobject DROP COLUMN pmra;
ALTER TABLE diaobject DROP COLUMN pmraerr;
ALTER TABLE diaobject DROP COLUMN pmra_parallax_cov;
ALTER TABLE diaobject DROP COLUMN pmdec;
ALTER TABLE diaobject DROP COLUMN pmdecerr;
ALTER TABLE diaobject DROP COLUMN pmdec_parallax_cov;
ALTER TABLE diaobject DROP COLUMN pmra_pmdec_cov;
ALTER TABLE diaobject ADD COLUMN validitystartmjdtai double precision;

ALTER TABLE ppdb_diaobject DROP COLUMN validitystart;
ALTER TABLE ppdb_diaobject DROP COLUMN validityend;
ALTER TABLE ppdb_diaobject DROP COLUMN radecmjdtai;
ALTER TABLE ppdb_diaobject DROP COLUMN parallax;
ALTER TABLE ppdb_diaobject DROP COLUMN parallaxerr;
ALTER TABLE ppdb_diaobject DROP COLUMN pmra;
ALTER TABLE ppdb_diaobject DROP COLUMN pmraerr;
ALTER TABLE ppdb_diaobject DROP COLUMN pmra_parallax_cov;
ALTER TABLE ppdb_diaobject DROP COLUMN pmdec;
ALTER TABLE ppdb_diaobject DROP COLUMN pmdecerr;
ALTER TABLE ppdb_diaobject DROP COLUMN pmdec_parallax_cov;
ALTER TABLE ppdb_diaobject DROP COLUMN pmra_pmdec_cov;
ALTER TABLE ppdb_diaobject ADD COLUMN validitystartmjdtai double precision;


-- Even though the DP1 database didn't have ids for forced sources,
--   the alert schema we're seeing *does*.
-- However, don't trust either diaSourceId or diaForcedSourceId as a primary
--   key.  Store them and index them, though.

-- Do not make parentdiasourceid a foreign key because
--   we may well get a source in an alert where the
--   parent source is not yet ingested.
ALTER TABLE diasource DROP COLUMN psfra;
ALTER TABLE diasource DROP COLUMN psfraerr;
ALTER TABLE diasource DROP COLUMN psfdec;
ALTER TABLE diasource DROP COLUMN psfdecerr;
ALTER TABLE diasource DROP COLUMN psfra_psfdec_cov;
ALTER TABLE diasource DROP COLUMN psfflux_psfra_cov;
ALTER TABLE diasource DROP COLUMN psfflux_psfdec_cov;
ALTER TABLE diasource DROP COLUMN ixxerr;
ALTER TABLE diasource DROP COLUMN iyyerr;
ALTER TABLE diasource DROP COLUMN ixyerr;
ALTER TABLE diasource DROP COLUMN ixx_ixy_cov;
ALTER TABLE diasource DROP COLUMN ixx_iyy_cov;
ALTER TABLE diasource DROP COLUMN iyy_ixy_cov;
ALTER TABLE diasource RENAME COLUMN fpbkgd TO templateflux;
ALTER TABLE diasource RENAME COLUMN fpbkgderr TO templatefluxerr;
ALTER TABLE diasource ALTER COLUMN x SET NOT NULL;
ALTER TABLE diasource ALTER COLUMN y SET NOT NULL;
ALTER TABLE diasource ALTER COLUMN psfflux DROP NOT NULL;
ALTER TABLE diasource ALTER COLUMN psffluxerr DROP NOT NULL;
ALTER TABLE diasource ADD COLUMN diasourceid bigint NOT NULL;
ALTER TABLE diasource ADD COLUMN parentdiasourceid bigint;
ALTER TABLE diasource ADD COLUMN apflux real;
ALTER TABLE diasource ADD COLUMN apfluxerr real;
ALTER TABLE diasource ADD COLUMN bboxsize int;
ALTER TABLE diasource ADD COLUMN timeprocessedmjdtai double precision;
ALTER TABLE diasource ADD COLUMN timewithdrawnmjdtai double precision;
ALTER TABLE diasource ALTER COLUMN flags SET DEFAULT 0;
ALTER TABLE diasource ALTER COLUMN pixelflags SET DEFAULT 0;
UPDATE diasource SET flags=0 WHERE flags IS NULL;
UPDATE diasource SET pixelflags=0 WHERE pixelflags IS NULL;
-- Not going to keep the trail and dipole stuff for now
-- Ideally broker filters will have gotten rid of things that are dipoles or trails
-- ALTER TABLE diasource ADD COLUMN trailflux real;
-- ALTER TABLE diasource ADD COLUMN trailfluxerr real;
-- ALTER TABLE diasource ADD COLUMN trailra double precision;
-- ALTER TABLE diasource ADD COLUMN trailraerr real;
-- ALTER TABLE diasource ADD COLUMN traildec double precision;
-- ALTER TABLE diasource ADD COLUMN traildecerr real;
-- ALTER TABLE diasource ADD COLUMN traillength real;
-- ALTER TABLE diasource ADD COLUMN traillengtherr real;
-- ALTER TABLE diasource ADD COLUMN trailangle real;
-- ALTER TABLE diasource ADD COLUMN trailangleerr real;
-- ALTER TABLE diasource ADD COLUMN trailchi2 real;
-- ALTER TABLE diasource ADD COLUMN tranndata int;
-- ALTER TABLE diasource ADD COLUMN trail_flag_edge bool;
-- ALTER TABLE diasource ADD COLUMN dipolemeanflux real;
-- ALTER TABLE diasource ADD COLUMN dipolemeanfluxerr real;
-- ALTER TABLE diasource ADD COLUMN dipoleflxudiff real;
-- ALTER TABLE diasource ADD COLUMN dipolefluxdifferr real;
-- ALTER TABLE diasource ADD COLUMN dipolelength real;
-- ALTER TABLE diasource ADD COLUMN dipoleangle real;
-- ALTER TABLE diasource ADD COLUMN dipolechi2 real;
-- ALTER TABLE diasource ADD COLUMN dipolendata int;
CREATE INDEX idx_diasource_diasourceid ON diasource(diasourceid);

ALTER TABLE ppdb_diasource DROP COLUMN psfra;
ALTER TABLE ppdb_diasource DROP COLUMN psfraerr;
ALTER TABLE ppdb_diasource DROP COLUMN psfdec;
ALTER TABLE ppdb_diasource DROP COLUMN psfdecerr;
ALTER TABLE ppdb_diasource DROP COLUMN psfra_psfdec_cov;
ALTER TABLE ppdb_diasource DROP COLUMN psfflux_psfra_cov;
ALTER TABLE ppdb_diasource DROP COLUMN psfflux_psfdec_cov;
ALTER TABLE ppdb_diasource DROP COLUMN ixxerr;
ALTER TABLE ppdb_diasource DROP COLUMN iyyerr;
ALTER TABLE ppdb_diasource DROP COLUMN ixyerr;
ALTER TABLE ppdb_diasource DROP COLUMN ixx_ixy_cov;
ALTER TABLE ppdb_diasource DROP COLUMN ixx_iyy_cov;
ALTER TABLE ppdb_diasource DROP COLUMN iyy_ixy_cov;
ALTER TABLE ppdb_diasource RENAME COLUMN fpbkgd TO templateflux;
ALTER TABLE ppdb_diasource RENAME COLUMN fpbkgderr TO templatefluxerr;
ALTER TABLE diasource ALTER COLUMN x SET NOT NULL;
ALTER TABLE diasource ALTER COLUMN y SET NOT NULL;
ALTER TABLE diasource ALTER COLUMN psfflux DROP NOT NULL;
ALTER TABLE diasource ALTER COLUMN psffluxerr DROP NOT NULL;
ALTER TABLE ppdb_diasource ADD COLUMN diasourceid bigint;
ALTER TABLE ppdb_diasource ADD COLUMN parentdiasourceid bigint;
ALTER TABLE ppdb_diasource ADD COLUMN apflux real;
ALTER TABLE ppdb_diasource ADD COLUMN apfluxerr real;
ALTER TABLE ppdb_diasource ADD COLUMN bboxsize int;
ALTER TABLE ppdb_diasource ADD COLUMN timeprocessedmjdtai double precision;
ALTER TABLE ppdb_diasource ADD COLUMN timewithdrawnmjdtai double precision;
ALTER TABLE ppdb_diasource ALTER COLUMN flags SET DEFAULT 0;
ALTER TABLE ppdb_diasource ALTER COLUMN pixelflags SET DEFAULT 0;
ALTER TABLE ppdb_diasource ALTER COLUMN psfflux DROP NOT NULL;
ALTER TABLE ppdb_diasource ALTER COLUMN psffluxerr DROP NOT NULL;
UPDATE ppdb_diasource SET flags=0 WHERE flags IS NULL;
UPDATE ppdb_diasource SET pixelflags=0 WHERE pixelflags IS NULL;
-- Not going to keep the trail and dipole stuff for now
-- Ideally broker filters will have gotten rid of things that are dipoles or trails
-- ALTER TABLE ppdb_diasource ADD COLUMN trailflux real;
-- ALTER TABLE ppdb_diasource ADD COLUMN trailfluxerr real;
-- ALTER TABLE ppdb_diasource ADD COLUMN trailra double precision;
-- ALTER TABLE ppdb_diasource ADD COLUMN trailraerr real;
-- ALTER TABLE ppdb_diasource ADD COLUMN traildec double precision;
-- ALTER TABLE ppdb_diasource ADD COLUMN traildecerr real;
-- ALTER TABLE ppdb_diasource ADD COLUMN traillength real;
-- ALTER TABLE ppdb_diasource ADD COLUMN traillengtherr real;
-- ALTER TABLE ppdb_diasource ADD COLUMN trailangle real;
-- ALTER TABLE ppdb_diasource ADD COLUMN trailangleerr real;
-- ALTER TABLE ppdb_diasource ADD COLUMN trailchi2 real;
-- ALTER TABLE ppdb_diasource ADD COLUMN tranndata int;
-- ALTER TABLE ppdb_diasource ADD COLUMN trail_flag_edge bool;
-- ALTER TABLE ppdb_diasource ADD COLUMN dipolemeanflux real;
-- ALTER TABLE ppdb_diasource ADD COLUMN dipolemeanfluxerr real;
-- ALTER TABLE ppdb_diasource ADD COLUMN dipoleflxudiff real;
-- ALTER TABLE ppdb_diasource ADD COLUMN dipolefluxdifferr real;
-- ALTER TABLE ppdb_diasource ADD COLUMN dipolelength real;
-- ALTER TABLE ppdb_diasource ADD COLUMN dipoleangle real;
-- ALTER TABLE ppdb_diasource ADD COLUMN dipolechi2 real;
-- ALTER TABLE ppdb_diasource ADD COLUMN dipolendata int;
CREATE INDEX idx_ppdb_diasource_diasourceid ON ppdb_diasource(diasourceid);


ALTER TABLE diaforcedsource ALTER COLUMN band DROP NOT NULL;
ALTER TABLE diaforcedsource ALTER COLUMN psfflux DROP NOT NULL;
ALTER TABLE diaforcedsource ALTER COLUMN psffluxerr DROP NOT NULL;
ALTER TABLE diaforcedsource DROP COLUMN time_processed;
ALTER TABLE diaforcedsource DROP COLUMN time_withdrawn;
ALTER TABLE diaforcedsource ADD COLUMN timeprocessedmjdtai double precision NOT NULL;
ALTER TABLE diaforcedsource ADD COLUMN timewithdrawnmjdtai double precision;
ALTER TABLE diaforcedsource ADD COLUMN diaforcedsourceid bigint;
CREATE INDEX idx_diaforcedsource_diaforcedsourceid ON diaforcedsource(diaforcedsourceid);


ALTER TABLE ppdb_diaforcedsource ALTER COLUMN band DROP NOT NULL;
ALTER TABLE ppdb_diaforcedsource ALTER COLUMN psfflux DROP NOT NULL;
ALTER TABLE ppdb_diaforcedsource ALTER COLUMN psffluxerr DROP NOT NULL;
ALTER TABLE ppdb_diaforcedsource DROP COLUMN time_processed;
ALTER TABLE ppdb_diaforcedsource DROP COLUMN time_withdrawn;
ALTER TABLE ppdb_diaforcedsource ADD COLUMN timeprocessedmjdtai double precision NOT NULL;
ALTER TABLE ppdb_diaforcedsource ADD COLUMN timewithdrawnmjdtai double precision;
ALTER TABLE ppdb_diaforcedsource ADD COLUMN diaforcedsourceid bigint;
CREATE INDEX idx_ppdb_diaforcedsource_diaforcedsourceid ON ppdb_diaforcedsource(diaforcedsourceid);
