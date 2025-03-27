-- These are tables that simulate the PPDB from an SNANA sim (created
--   specifically thinking about ELAsTiCC2).
-- They have almost the same structure as diaobject, diasource, and diaforcedsource
--   since I already wrote code to load SNANA into those...  No
--   processing versions here, though.

CREATE TABLE ppdb_host_galaxy(
  id UUID PRIMARY KEY NOT NULL DEFAULT gen_random_uuid(),
  objectid bigint NOT NULL,
  psradectai real,
  psra double precision,
  psdec double precision,
  stdcolor_u real,
  stdcolor_g real,
  stdcolor_r real,
  stdcolor_i real,
  stdcolor_z real,
  stdcolor_y real,
  stdcolor_u_err real,
  stdcolor_g_err real,
  stdcolor_r_err real,
  stdcolor_i_err real,
  stdcolor_z_err real,
  stdcolor_y_err real,
  pzmode real,
  pzmean real,
  pzstd real,
  pzskew real,
  pskurt real,
  pzquant000 real,
  pzquant010 real,
  pzquant020 real,
  pzquant030 real,
  pzquant040 real,
  pzquant050 real,
  pzquant060 real,
  pzquant070 real,
  pzquant080 real,
  pzquant090 real,
  pzquant100 real,
  flags bigint
);
CREATE INDEX idx_ppdb_hostgalaxy_objectid ON ppdb_host_galaxy(objectid);


CREATE TABLE ppdb_diaobject(
  diaobjectid bigint NOT NULL PRIMARY KEY,
  radecmjdtai real,
  validitystart timestamp with time zone,
  validityend timestamp with time zone,
  ra double precision NOT NULL,
  raerr real,
  dec double precision NOT NULL,
  decerr real,
  ra_dec_cov real,
  nearbyextobj1 bigint,
  nearbyextobj1id UUID,
  nearbyextobj1sep real,
  nearbyextobj2 bigint,
  nearbyextobj2id UUID,
  nearbyextobj2sep real,
  nearbyextobj3 bigint,
  nearbyextobj3id UUID,
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
  pmra_pmdec_cov real
);
CREATE INDEX idx_ppdb_diaobject_nearbyext1 ON ppdb_diaobject(nearbyextobj1id);
ALTER TABLE ppdb_diaobject ADD CONSTRAINT fk_ppdb_diaobject_nearbyext1
  FOREIGN KEY (nearbyextobj1id) REFERENCES ppdb_host_galaxy(id) ON DELETE SET NULL;
CREATE INDEX idx_ppdb_diaobject_nearbyext2 ON ppdb_diaobject(nearbyextobj2id);
ALTER TABLE ppdb_diaobject ADD CONSTRAINT fk_ppdb_diaobject_nearbyext2
  FOREIGN KEY (nearbyextobj2id) REFERENCES ppdb_host_galaxy(id) ON DELETE SET NULL;
CREATE INDEX idx_ppdb_diaobject_nearbyext3 ON ppdb_diaobject(nearbyextobj3id);
ALTER TABLE ppdb_diaobject ADD CONSTRAINT fk_ppdb_diaobject_nearbyext3
  FOREIGN KEY (nearbyextobj3id) REFERENCES ppdb_host_galaxy(id) ON DELETE SET NULL;


CREATE TABLE ppdb_diasource(
  diasourceid bigint NOT NULL PRIMARY KEY,
  diaobjectid bigint NOT NULL,
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
  pixelflags integer
);
CREATE INDEX idx_ppdb_diasource_diaobjectid ON ppdb_diasource(diaobjectid);
ALTER TABLE ppdb_diasource ADD CONSTRAINT fk_ppdb_diasource_diaobject
  FOREIGN KEY (diaobjectid) REFERENCES ppdb_diaobject(diaobjectid) ON DELETE CASCADE;


CREATE TABLE ppdb_diaforcedsource (
  diaforcedsourceid bigint NOT NULL PRIMARY KEY,
  diaobjectid bigint NOT NULL,
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
  time_withdrawn timestamp with time zone
);
CREATE INDEX idx_ppdb_diaforcedsource_diaobjectid ON ppdb_diaforcedsource(diaobjectid);
ALTER TABLE ppdb_diaforcedsource ADD CONSTRAINT fk_ppdb_diaforcedsource_diaobject
  FOREIGN KEY (diaobjectid) REFERENCES ppdb_diaobject(diaobjectid) ON DELETE CASCADE;
