-- *******************************************************************************************************
-- HOST GALAXIES AND DIAOBJECT

-- Punting on old host galaxies.  We don't have any tables where they need to be preserved at the moment.

ALTER TABLE diaobject DROP CONSTRAINT fk_diaobject_nearbyext1;
ALTER TABLE diaobject DROP CONSTRAINT fk_diaobject_nearbyext2;
ALTER TABLE diaobject DROP CONSTRAINT fk_diaobject_nearbyext3;
DROP TABLE host_galaxy;
CREATE TABLE host_galaxy(
    id                         uuid PRIMARY KEY,
    host_catalog               text,
    host_id                    text,
    base_procver_id            uuid,
    prio                       smallint,
    ra                         double precision,
    dec                        double precision,
    info                       JSONB
);
COMMENT ON COLUMN host_galaxy.id IS 'just a gratuitous primary key';
COMMENT ON COLUMN host_galaxy.host_catalog IS 'indication of what catalog this host is from';
COMMENT ON COLUMN host_galaxy.host_id IS 'some kind of identification of the host within host_catalog';
COMMENT ON COLUMN host_galaxy.base_procver_id IS 'base processing version for this host';
COMMENT ON COLUMN host_galaxy.ra IS 'ra of host';
COMMENT ON COLUMN host_galaxy.dec IS 'dec of host';
COMMENT ON COLUMN host_galaxy.info IS 'catalog / procver dependent additional info about host';
CREATE UNIQUE INDEX idx_hostgalaxy_specifier ON host_galaxy( host_catalog, host_id, base_procver_id );
CREATE INDEX idx_hostgalaxy_host_catalog ON host_galaxy( host_catalog );
CREATE INDEX idx_hostgalaxy_base_procver_id ON host_galaxy( base_procver_id );
CREATE INDEX idx_hostgalaxy_q3c ON host_galaxy( q3c_ang2ipix(ra, dec) );
ALTER TABLE host_galaxy ADD CONSTRAINT fk_host_galaxy_base_procver
  FOREIGN KEY (base_procver_id) REFERENCES base_processing_version(id);


CREATE TABLE diaobject_host_match(
    diaobjectid                bigint,
    host_galaxy_id             uuid,
    base_procver_id            uuid,
    prio                       smallint
);
COMMENT ON COLUMN diaobject_host_match.diaobjectid IS 'Link back to diaobject table';
COMMENT ON COLUMN diaobject_host_match.host_galaxy_id IS 'Link back to host_galaxy table';
COMMENT ON COLUMN diaobject_host_match.base_procver_id IS 'base processing version for this host match';
COMMENT ON COLUMN diaobject_host_match.prio IS
   'Sorted from 0 (high) to 32767 (low), priority ranking of host.  NULL=no ranking';
ALTER TABLE diaobject_host_match ADD CONSTRAINT pk_diaobject_host_match
  PRIMARY KEY( diaobjectid, host_galaxy_id, base_procver_id );
CREATE INDEX idx_diaobject_host_match_diaobjectid ON diaobject_host_match(diaobjectid);
CREATE INDEX idx_diaobject_host_match_host_galaxy_id ON diaobject_host_match(host_galaxy_id);
CREATE INDEX idx_diaobject_host_match_base_procver_id ON diaobject_host_match(base_procver_id);
ALTER TABLE diaobject_host_match ADD CONSTRAINT fk_diaobject_host_match_diaobject
  FOREIGN KEY (diaobjectid) REFERENCES diaobject(diaobjectid);
ALTER TABLE diaobject_host_match ADD CONSTRAINT fk_diaobject_host_match_host_galaxy
  FOREIGN KEY (host_galaxy_id) REFERENCES host_galaxy(id);
ALTER TABLE diaobject_host_match ADD CONSTRAINT fk_diaobject_host_match_procver
  FOREIGN KEY (base_procver_id) REFERENCES base_processing_version(id);


-- Move diaobject positions out to their own table so they can have their own processing version

CREATE TABLE diaobject_position(
  diaobjectid                bigint NOT NULL,
  base_procver_id            uuid NOT NULL,
  ra                         double precision NOT NULL, 
  dec                        double precision NOT NULL,
  raerr                      real,
  decerr                     real,
  ra_dec_cov                 real,
  created_at                 timestamp with time zone DEFAULT NOW()
);
COMMENT ON COLUMN diaobject_position.diaobjectid IS 'Link back to diaobject table';
COMMENT ON COLUMN diaobject_position.base_procver_id IS 'base processing version for the position';
COMMENT ON COLUMN diaobject_position.ra IS 'ra';
COMMENT ON COLUMN diaobject_position.dec IS 'dec';
COMMENT ON COLUMN diaobject_position.raerr IS 'uncertainty (NOT variance) on ra';
COMMENT ON COLUMN diaobject_position.decerr IS 'uncertainty (NOT variance) on dec';
COMMENT ON COLUMN diaobject_position.ra_dec_cov IS 'covariance between ra and dec';
ALTER TABLE diaobject_position ADD CONSTRAINT pk_diaobject_position
  PRIMARY KEY( diaobjectid, base_procver_id );
CREATE INDEX idx_diaobject_position_diaobjectid ON diaobject_position( diaobjectid );
CREATE INDEX idx_diaobject_base_procver_id ON diaobject_position( base_procver_id );
ALTER TABLE diaobject_position ADD CONSTRAINT fk_diaobject_position_diaobject
  FOREIGN KEY (diaobjectid) REFERENCES diaobject(diaobjectid);
ALTER TABLE diaobject_position ADD CONSTRAINT fk_diaobject_position_procver
  FOREIGN KEY (base_procver_id) REFERENCES base_processing_version(id);

-- Create a new base processing version for positions, copy all positions out of the diaobject table

CREATE TEMPORARY TABLE tmp_bpvid( id uuid );
INSERT INTO tmp_bpvid(id) VALUES ( gen_random_uuid() );
INSERT INTO base_processing_version(id, description)
  SELECT id, 'migration to diaobject_position table' FROM tmp_bpvid;
INSERT INTO diaobject_position(diaobjectid, base_procver_id, ra, dec, raerr, decerr, ra_dec_cov)
  SELECT o.diaobjectid, t.id, o.ra, o.dec, o.raerr, o.decerr, o.ra_dec_cov
  FROM diaobject o
  LEFT JOIN tmp_bpvid t ON TRUE;
DROP TABLE tmp_bpvid;  

-- Remove columns from the diaobject table
ALTER TABLE diaobject DROP COLUMN ra;
ALTER TABLE diaobject DROP COLUMN dec;
ALTER TABLE diaobject DROP COLUMN raerr;
ALTER TABLE diaobject DROP COLUMN decerr;
ALTER TABLE diaobject DROP COLUMN ra_dec_cov;
ALTER TABLE diaobject DROP COLUMN nearbyextobj1;
ALTER TABLE diaobject DROP COLUMN nearbyextobj1id;
ALTER TABLE diaobject DROP COLUMN nearbyextobj1sep;
ALTER TABLE diaobject DROP COLUMN nearbyextobj2;
ALTER TABLE diaobject DROP COLUMN nearbyextobj2id;
ALTER TABLE diaobject DROP COLUMN nearbyextobj2sep;
ALTER TABLE diaobject DROP COLUMN nearbyextobj3;
ALTER TABLE diaobject DROP COLUMN nearbyextobj3id;
ALTER TABLE diaobject DROP COLUMN nearbyextobj3sep;
ALTER TABLE diaobject DROP COLUMN nearbylowzgal;
ALTER TABLE diaobject DROP COLUMN nearbylowzgalsep;
ALTER TABLE diaobject DROP COLUMN validitystartmjdtai;
COMMENT ON COLUMN diaobject.diaobjectid IS 'Globally unique (across all proc vers) diaobject id';
COMMENT ON COLUMN diaobject.base_procver_id IS 'base processing version for this diaobject';
COMMENT ON COLUMN diaobject.rootid IS 'root_diaobject id for this object';

-- **********************************************************************
-- diasource table
-- Divide this into basic info (just the lightcurve) plus "extended info" that we may or may not have

ALTER TABLE diasource RENAME TO diasource_old;
-- Drop some indexes so we can reuse the names
DROP INDEX idx_diasource_diasourceid;
DROP INDEX idx_diasource_diaobjectid;
DROP INDEX idx_diasource_visit;
DROP INDEX idx_diasource_band;
DROP INDEX idx_diasource_mjd;

CREATE TABLE diasource(
  diasourceid         bigint NOT NULL,
  base_procver_id     uuid NOT NULL,
  diaobjectid         bigint NOT NULL,
  visit               bigint NOT NULL,
  band                character(1) NOT NULL,
  midpointmjdtai      double precision NOT NULL,
  psfflux             real NOT NULL,
  psffluxerr          real NOT NULL
);
COMMENT ON COLUMN diasource.diasourceid IS 'id of this source, unique within base_procver_id';
COMMENT ON COLUMN diasource.base_procver_id IS 'base proc ver of this source';
COMMENT ON COLUMN diasource.diaobjectid IS 'diaobject of this source';
COMMENT ON COLUMN diasource.visit IS 'visit of this source; (base_procver_id,diaobjectid,visit) is unique';
ALTER TABLE diasource ADD CONSTRAINT pk_diasource PRIMARY KEY( diasourceid, base_procver_id );
CREATE UNIQUE INDEX pk_diasource_spec ON diasource( diaobjectid, visit, base_procver_id );
CREATE INDEX idx_diasource_diasourceid ON diasource( diasourceid );
CREATE INDEX idx_diasource_diaobjectid ON diasource( diaobjectid );
CREATE INDEX idx_diasource_visit ON diasource( visit );
CREATE INDEX idx_diasource_band ON diasource( band );
CREATE INDEX idx_diasource_base_procver_id ON diasource( base_procver_id );
CREATE INDEX idx_diasource_mjd ON diasource( midpointmjdtai );
ALTER TABLE diasource ADD CONSTRAINT fk_diasource_diaobject
  FOREIGN KEY (diaobjectid) REFERENCES diaobject( diaobjectid );
ALTER TABLE diasource ADD CONSTRAINT fk_diasource_base_procver
  FOREIGN KEY (base_procver_id) REFERENCES base_processing_version( id );

CREATE TABLE diasource_extra(
  diasourceid          bigint NOT NULL,
  base_procver_id      uuid NOT NULL,
  detector             smallint,
  x                    real,
  y                    real,
  xerr                 real,
  yerr                 real,
  x_y_cov              real,
  ra                   double precision,
  raerr                real,
  dec                  double precision,
  decerr               real,
  ra_dec_cov           real,
  psfflux              real,
  psffluxerr           real,
  psflnl               real,
  psfchi2              real,
  psfndata             integer,
  snr                  real,
  scienceflux          real,
  sciencefluxerr       real,
  templateflux         real,
  templatefluxerr      real,
  extendedness         real,
  reliability          real,
  ixx                  real,
  iyy                  real,
  ixy                  real,
  ixxpsf               real,
  iyypsf               real,
  ixypsf               real,
  flags                integer DEFAULT 0,
  pixelflags           integer DEFAULT 0,
  apflux               real,
  apfluxerr            real,
  bboxsize             integer,
  timeprocessedmjdtai  double precision,
  timewithdrawnmjdtai  double precision,
  parentdiasourceid    bigint
);
COMMENT ON COLUMN diasource.diasourceid IS 'with base_procver_id, link to diasource table';
COMMENT ON COLUMN diasource.base_procver_id IS 'with diasourceid, link to diasource table';
ALTER TABLE diasource_extra ADD PRIMARY KEY ( diasourceid, base_procver_id );
ALTER TABLE diasource_extra ADD CONSTRAINT fk_diasource_extra_diasource
  FOREIGN KEY ( diasourceid, base_procver_id ) REFERENCES diasource( diasourceid, base_procver_id );
CREATE INDEX idx_diasource_extra_q3c ON diasource_extra( q3c_ang2ipix( ra, dec ) );

INSERT INTO diasource(diasourceid, base_procver_id, diaobjectid, visit, band, midpointmjdtai, psfflux, psffluxerr)
  SELECT diasourceid, base_procver_id, diaobjectid, visit, band, midpointmjdtai, psfflux, psffluxerr
  FROM diasource_old;

INSERT INTO diasource_extra(diasourceid, base_procver_id, detector, x, y, xerr, yerr, x_y_cov,
                            ra, raerr, dec, decerr, ra_dec_cov,
                            psfflux, psffluxerr, psflnl, psfchi2, psfndata, snr,
                            scienceflux, sciencefluxerr, templateflux, templatefluxerr,
                            extendedness, reliability, ixx, iyy, ixy, ixxpsf, iyypsf, ixypsf,
                            flags, pixelflags, apflux, apfluxerr,                            
                            bboxsize, timeprocessedmjdtai, timewithdrawnmjdtai, parentdiasourceid)
  SELECT diasourceid, base_procver_id, detector, x, y, xerr, yerr, x_y_cov,
         ra, raerr, dec, decerr, ra_dec_cov,
         psfflux, psffluxerr, psflnl, psfchi2, psfndata, snr,
         scienceflux, sciencefluxerr, templateflux, templatefluxerr,
         extendedness, reliability, ixx, iyy, ixy, ixxpsf, iyypsf, ixypsf,
         flags, pixelflags, apflux, apfluxerr,                            
         bboxsize, timeprocessedmjdtai, timewithdrawnmjdtai, parentdiasourceid
  FROM diasource_old;

DROP TABLE diasource_old;


-- **********************************************************************
-- diaforcedsource table
-- Similar division to diasource

ALTER TABLE diaforcedsource RENAME TO diaforcedsource_old;
-- Drop some indexes so we can reuse the name
DROP INDEX idx_diaforcedsource_diaforcedsourceid;
DROP INDEX idx_diaforcedsource_diaobjectid;
DROP INDEX idx_diaforcedsource_visit;

CREATE TABLE diaforcedsource(
  diaforcedsourceid           bigint,
  base_procver_id             uuid NOT NULL,
  diaobjectid                 bigint NOT NULL,
  visit                       bigint NOT NULL,
  band                        character(1) NOT NULL,
  midpointmjdtai              double precision NOT NULL,
  psfflux                     real NOT NULL,
  psffluxerr                  real not NULL
);
COMMENT ON COLUMN diaforcedsource.diaforcedsourceid IS 'id of this diaforcedsource; scary, DP1 omitted it';
COMMENT ON COLUMN diaforcedsource.base_procver_id IS 'base proc ver of this forced source';
COMMENT ON COLUMN diaforcedsource.diaobjectid IS 'diaobject of this forced source';
COMMENT ON COLUMN diaforcedsource.visit IS 'visit of this source; (base_procver_id,diaobjectid,visit) is primary key';
ALTER TABLE diaforcedsource ADD CONSTRAINT pk_diaforcedsource PRIMARY KEY (base_procver_id, diaobjectid, visit);
CREATE INDEX idx_diaforcedsourceid ON diaforcedsource( diaforcedsourceid );
CREATE INDEX idx_diaforcedsource_diaobjectid ON diaforcedsource( diaobjectid );
CREATE INDEX idx_diaforcedsource_visit ON diaforcedsource( visit );
CREATE INDEX idx_diaforcedsource_base_procver_id ON diaforcedsource( base_procver_id );
CREATE INDEX idx_diaforcedsource_mjd ON diaforcedsource( midpointmjdtai );
ALTER TABLE diaforcedsource ADD CONSTRAINT fk_diaforcedsource_diaobject
  FOREIGN KEY (diaobjectid) references diaobject( diaobjectid );
ALTER TABLE diaforcedsource ADD CONSTRAINT fk_diaforcedsource_base_procver
  FOREIGN KEY (base_procver_id) REFERENCES base_processing_version( id );

CREATE TABLE diaforcedsource_extra(
  diaobjectid                 bigint NOT NULL,
  visit                       bigint NOT NULL,
  base_procver_id             uuid NOT NULL,
  detector                    smallint,
  ra                          double precision,
  dec                         double precision,
  psfflux                     real,
  psffluxerr                  real,
  scienceflux                 real,
  sciencefluxerr              real,
  timeprocessedmjdtai         double precision,
  timewithdrawnmjdtai         double precision
);
ALTER TABLE diaforcedsource_extra ADD PRIMARY KEY (base_procver_id, diaobjectid, visit);
ALTER TABLE diaforcedsource_extra ADD CONSTRAINT fk_diaforcedsource_extra_diaforcedsource
  FOREIGN KEY (base_procver_id, diaobjectid, visit)
  REFERENCES diaforcedsource( base_procver_id, diaobjectid, visit );

INSERT INTO diaforcedsource(diaforcedsourceid, base_procver_id, diaobjectid, visit, band,
                            midpointmjdtai, psfflux, psffluxerr)
  SELECT diaforcedsourceid, base_procver_id, diaobjectid, visit, band, midpointmjdtai, psfflux, psffluxerr
  FROM diaforcedsource_old;
INSERT INTO diaforcedsource_extra(diaobjectid, visit, base_procver_id, detector, ra, dec, psfflux, psffluxerr,
                                  scienceflux, sciencefluxerr, timeprocessedmjdtai, timewithdrawnmjdtai)
  SELECT diaobjectid, visit, base_procver_id, detector, ra, dec, psfflux, psffluxerr,
         scienceflux, sciencefluxerr, timeprocessedmjdtai, timewithdrawnmjdtai
  FROM diaforcedsource_old;

DROP TABLE diaforcedsource_old;

-- **********************************************************************
-- What we can expect from spectrum people is less than what we want
-- Try to be flexible

ALTER TABLE spectruminfo DROP CONSTRAINT fk_spectruminfo_root_diaobject;
ALTER TABLE spectruminfo ALTER COLUMN root_diaobject_id DROP NOT NULL;
ALTER TABLE spectruminfo ADD COLUMN ra double precision NOT NULL;
ALTER TABLE spectruminfo ADD COLUMN dec double precision NOT NULL;
ALTER TABLE spectruminfo ADD COLUMN is_host boolean NOT NULL;
ALTER TABLE spectruminfo ADD COLUMN class_description text;
CREATE INDEX idx_spectruminfo_q3c ON spectruminfo( q3c_ang2ipix(ra, dec) );
CREATE INDEX idx_spectruminfo_facility ON spectruminfo( facility );
