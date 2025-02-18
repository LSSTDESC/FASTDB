-- Tables used for the rkauth system
CREATE TABLE authuser(
  id UUID NOT NULL DEFAULT gen_random_uuid(),
  username text NOT NULL,
  displayname text NOT NULL,
  email text NOT NULL,
  pubkey text,
  privkey jsonb
);
ALTER TABLE authuser ADD CONSTRAINT pk_authuser PRIMARY KEY (id);
CREATE UNIQUE INDEX ix_authuser_username ON authuser USING btree (username);
CREATE INDEX ix_authuser_email ON authuser USING btree(email);

CREATE TABLE passwordlink(
  id UUID NOT NULL,
  userid UUID NOT NULL,
  expires timestamp with time zone
);
ALTER TABLE passwordlink ADD CONSTRAINT pk_passwordlink PRIMARY KEY (id);
CREATE INDEX ix_passwordlink_userid ON passwordlink USING btree (userid);


-- ProcessingVersion
-- Most things are tagged with a processing version so that
--   we can have multiple versions of the same thing
CREATE TABLE processingversion(
  id integer PRIMARY KEY,
  description text,
  validity_start timestamp with time zone NOT NULL,
  validity_end timestamp with time zone
);
CREATE INDEX idx_processingversion_desc ON processingversion(description);  

-- SnapShot
-- Can define a set of objects by tagging the processing version and thing id
CREATE TABLE snapshot(
  id INTEGER PRIMARY KEY,
  description text,
  creation_time timestamp with time zone DEFAULT NOW()
);
CREATE INDEX idx_snapshot_desc ON snapshot(description);


-- Selected from the APDB table from
--   https://sdm-schemas.lsst.io/apdb.html
-- NOTE: diaobjectid was convereted to bigint from long
--   for compatibility with SNANA elasticc
-- Not making diaobjectid the primary key because
--   we expect LSST to change and reuse these
--   integers with different releases.
--   TODO : a table that collects together
--   diaobjects and identifies them all as
--   the same thing
CREATE TABLE diaobject(
  id UUID PRIMARY KEY NOT NULL DEFAULT gen_random_uuid(),
  processing_version integer NOT NULL,
  diaobjectid bigint NOT NULL,
  radecmjdtai real,
  validitystart timestamp with time zone,
  validityend timestamp with time zone,
  ra double precision NOT NULL,
  raerr real,
  dec double precision NOT NULL,
  decerr real,
  ra_dec_cov real,
  nearbyextobj1 integer,
  nearbyextobj1sep real,
  nearbyextobj2 integer,
  nearbyextobj2sep real,
  nearbyextobj3 integer,
  nearbyextobj3sep real,
  nearbylowzgal text,
  nearbylowzgalsep real,
  parallax real,
  parallaxerr real,
  pmra real,
  pmraerr real,
  pmra_parallax_cov real,
  pmdec real,
  pmdecerr real,
  pmdec_parallax_cov real,
  pm_ra_dec_cov real
);
CREATE INDEX idx_diaobject_q3c ON diaobject (q3c_ang2ipix(ra, dec));
CREATE INDEX idx_diaobject_diaobjectid ON diaobject(diaobjectid);
CREATE INDEX idx_diaobject_procver ON diaobject(processing_version);
ALTER TABLE diaobject ADD CONSTRAINT fk_diaobject_procver
  FOREIGN KEY (processing_version) REFERENCES processingversion(id) ON DELETE RESTRICT;

-- Selected from DiaSource APDB table
-- Flags converted to the flags bitfield:
--   centroid_flag : 2^0
--   forced_psfflux_flag : 2^1
--   forcedpsf_flux_edge_flag: 2^2
--   is_negative: 2^3
--   isdipole: 2^4
--   psfflux_flag: 2^5
--   psfflux_flag_edge: 2^6
--   psfflux_flag_nogoodpixels: 2^7
--   shape_flag: 2^8
--   shape_flag_no_pixels: 2^9
--   shape_flag_not_contained: 2^10
--   shape_flag_parent_source: 2^11
--   trail_flag_edge: 2^12

-- Flags converted to the pixelflags bitfield
--   pixelflags: 2^0
--   pixelflags_bad: 2^1
--   pixelflags_cr: 2^2
--   pixelflags_crcenter: 2^3
--   pixelflags_edge: 2^4
--   pixelflags_injected: 2^5
--   pixelflags_injectedtemplate: 2^6
--   pixelflags_injected_templatecenter: 2^7
--   pixelflags_injectedcenter: 2^8
--   pixelflags_interpolated: 2^9
--   pixelflags_interpolatedcetner: 2^10
--   pixelflags_offimage: 2^11
--   pixelflags_saturated: 2^12
--   pixelflags_saturatedcenter: 2^13
--   pixelflags_streak: 2^14
--   pixelflags_streakcenter: 2^15
--   pixelflags_suspect: 2^16
--   pixelflags_suspectcenter: 2^17

CREATE TABLE diasource(
  diasourceid bigint NOT NULL,
  processing_version integer NOT NULL,
  diaobjectuuid UUID,
  diaobjectid bigint,
  ssobjectid bigint,
  visit integer NOT NULL,
  detector smallint NOT NULL,
  x real,
  y real,
  xerr real,
  yerr real,
  x_y_cov real,

  band char NOT NULL,
  midpointmjdtai double precision NOT NULL,
  ra double precision NOT NULL,
  raerr real,
  dec double precision NOT NULL,
  decerr real,
  ra_dec_cov real,

  psfflux real NOT NULL,
  psffluxerr real NOT NULL,
  psfra double precision,
  psfraerr real,
  psfdec double precision,
  psfdecerr real,
  psfra_psfdec_cov real,
  psfflux_psfra_cov real,
  psfflux_psfdec_cov real,
  psflnl real,
  psfchi2 real,
  psfndata integer,
  snr real,

  scienceflux real,
  sciencefluxerr real,
  
  fpbkgd real,
  fpbkgderr real,

  parentdiasourceid bigint,
  extendedness real,
  reliability real,
  
  ixx real,
  ixxerr real,
  iyy real,
  iyyerr real,
  ixy real,
  ixyerr real,
  ixx_ixy_cov real,
  ixx_iyy_cov real,
  iyy_ixy_cov real,
  ixxpsf real,
  iyypsf real,
  ixypsf real,
  
  flags integer,
  pixelflags integer,

  PRIMARY KEY (diasourceid, processing_version)
)
PARTITION BY LIST (processing_version);
CREATE INDEX idx_diasource_id ON diasource(diasourceid);
CREATE INDEX idx_diasource_q3c ON diasource (q3c_ang2ipix(ra, dec));
CREATE INDEX idx_diasource_visit ON diasource(visit);
CREATE INDEX idx_diasource_detector ON diasource(detector);
CREATE INDEX idx_diasource_band ON diasource(band);
CREATE INDEX idx_diasource_mjd ON diasource(midpointmjdtai);
CREATE INDEX idx_diasource_diaobjectid ON diasource(diaobjectuuid);
ALTER TABLE diasource ADD CONSTRAINT fk_diasource_diaobjectid
  FOREIGN KEY (diaobjectuuid) REFERENCES diaobject(id) ON DELETE CASCADE;
CREATE INDEX idx_diasource_procver ON diasource(processing_version);
ALTER TABLE diasource ADD CONSTRAINT fk_diasource_procver
  FOREIGN KEY (processing_version) REFERENCES processingversion(id) ON DELETE RESTRICT;
  

-- Selected from DiaForcedSource APDB table
CREATE TABLE diaforcedsource (
  diaforcedsourceid bigint NOT NULL,
  diaobjectuuid UUID NOT NULL,
  processing_version integer NOT NULL,
  visit integer NOT NULL,
  detector smallint NOT NULL,
  midpointmjdtai double precision NOT NULL,
  band char NOT NULL,
  ra double precision NOT NULL,
  dec double precision NOT NULL,
  psfflux real NOT NULL,
  psffluxerr real NOT NULL,
  scienceflux real NOT NULL,
  sciencefluxerr real NOT NULL,
  time_processed timestamp with time zone,
  time_withdrawn timestamp with time zone,

  PRIMARY KEY (diaforcedsourceid, processing_version)
)
PARTITION BY LIST (processing_version);
CREATE INDEX idx_diaforcedsource_id ON diaforcedsource(diaforcedsourceid);
CREATE INDEX idx_diaforcedsource_q3c ON diaforcedsource (q3c_ang2ipix(ra, dec));
CREATE INDEX idx_diaforcedsource_visit ON diaforcedsource(visit);
CREATE INDEX idx_diaforcedsource_detector ON diaforcedsource(detector);
CREATE INDEX idx_diaforcedsource_mjdtai ON diaforcedsource(midpointmjdtai);
CREATE INDEX idx_diaforcedsource_band ON diaforcedsource(band);
ALTER TABLE diaforcedsource ADD CONSTRAINT fk_diaforcedsource_diaobjectid
  FOREIGN KEY (diaobjectuuid) REFERENCES diaobject(id) ON DELETE CASCADE;
CREATE INDEX idx_diaforcedsource_procver ON diaforcedsource(processing_version);
ALTER TABLE diaforcedsource ADD CONSTRAINT fk_diaforcedsource_procver
  FOREIGN KEY (processing_version) REFERENCES processingversion(id) ON DELETE RESTRICT;



CREATE TABLE diasource_snapshot(
  diasourceid bigint NOT NULL,
  processing_version integer NOT NULL,
  snapshot integer NOT NULL,
  PRIMARY KEY( diasourceid, processing_version, snapshot)
)
PARTITION BY LIST (processing_version);
CREATE INDEX ix_dsss_diasource ON diasource_snapshot(diasourceid,processing_version);
CREATE INDEX ix_dsss_snapshot ON diasource_snapshot(snapshot);
ALTER TABLE diasource_snapshot ADD CONSTRAINT fk_diasource_snapshot_source
  FOREIGN KEY (diasourceid, processing_version) REFERENCES diasource(diasourceid, processing_version)
  ON DELETE CASCADE;
ALTER TABLE diasource_snapshot ADD CONSTRAINT fk_diasource_snapshot_snapshot
  FOREIGN KEY (snapshot) REFERENCES snapshot(id) ON DELETE CASCADE;



CREATE TABLE diaforcedsource_snapshot(
  diaforcedsourceid bigint NOT NULL,
  processing_version integer NOT NULL,
  snapshot integer NOT NULL,
  PRIMARY KEY( diaforcedsourceid, processing_version, snapshot)
)
PARTITION BY LIST (processing_version);
CREATE INDEX ix_dfsss_diaforcedsource ON diaforcedsource_snapshot(diaforcedsourceid,processing_version);
CREATE INDEX ix_dfsss_snapshot ON diaforcedsource_snapshot(snapshot);
ALTER TABLE diaforcedsource_snapshot ADD CONSTRAINT fk_diaforcedsource_snapshot_forcedsource
  FOREIGN KEY (diaforcedsourceid, processing_version) REFERENCES diaforcedsource(diaforcedsourceid, processing_version)
  ON DELETE CASCADE;
ALTER TABLE diaforcedsource_snapshot ADD CONSTRAINT fk_diaforcedsource_snapshot_snapshot
  FOREIGN KEY (snapshot) REFERENCES snapshot(id) ON DELETE CASCADE;


CREATE TABLE migrations_applied(
  filename text,
  applied_time timestamp with time zone DEFAULT NOW()
);
INSERT INTO migrations_applied(filename) VALUES('2025-02-18_001_init.sql');
