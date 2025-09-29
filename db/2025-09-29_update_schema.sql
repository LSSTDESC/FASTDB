-- Update table schema based on the lsst v9.0 alert schema
-- It's not an exhaustive copy, but....
-- Note that the current version of alerts have NO host
--   association in diaObject.  Leave those columns in for now,
--   however, in ancitipation that we'll eventually have it
--   (if only with DRs).

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
ALTER TABLE diasource DROP COLUMN ixx_ixy_cov;
ALTER TABLE diasource DROP COLUMN ixx_iyy_cov;
ALTER TABLE diasource DROP COLUMN iyy_ixy_cov;
ALTER TABLE diasource RENAME COLUMN fpbkgd TO templateflux;
ALTER TABLE diasource RENAME COLUMN fpbkgderr TO templatefluxerr;
ALTER TABLE diasource ADD COLUMN diasourceid bigint;
ALTER TABLE diasource ADD COLUMN parentdiasourceid bigint;
ALTER TABLE diasource ADD COLUMN appflux real;
ALTER TABLE diasource ADD COLUMN appfluxerr real;
ALTER TABLE diasource ADD COLUMN bboxsize int;
ALTER TABLE diasource ADD COLUMN timeprocessedmjdtai double precision;
ALTER TABLE diasource ADD COLUMN timewithdrawnmjdtai double precision;
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

ALTER TABLE diaforcedsource DROP COLUMN time_processed;
ALTER TABLE diaforcedsource DROP COLUMN time_withdrawn;
ALTER TABLE diaforcedsource ADD COLUMN timeprocessedmjdtai double precision;
ALTER TABLE diaforcedsource ADD COLUMN timewithdrawnmjdtai double precision;
ALTER TABLE diaforcedsource ADD COLUMN diaforcedsourceid bigint;
CREATE INDEX idx_diaforcedsource_diaforcedsourceid ON diaforcedsource(diaforcedsourceid);
