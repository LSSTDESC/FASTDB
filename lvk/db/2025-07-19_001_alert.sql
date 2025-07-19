-- Table used to store Ligo Virgo KAGRA (LVK) alerts
CREATE TABLE lvk(
  alert_type text NOT NULL,
  time_created timestamp [ (p) ] with time zone NOT NULL,
  superevent_id text PRIMARY KEY NOT NULL
);

CREATE TABLE event(
  superevent_id text PRIMARY KEY REFERENCES lvk(superevent_id),
  "time" timestamp [ (p) ] with time zone NOT NULL,
  far double precision NOT NULL,
  significant boolean NOT NULL,
  instruments text[] NOT NULL,
  search text NOT NULL,
  group text NOT NULL,
  pipeline text NOT NULL,
  duration double precision,
  central_frequency double precision
);

CREATE TABLE properties(
  superevent_id text PRIMARY KEY REFERENCES lvk(superevent_id),
  HasNS double precision,
  HasRemnant double precision,
  HasMassGap double precision
);

CREATE TABLE classification(
  superevent_id text PRIMARY KEY REFERENCES lvk(superevent_id),
  BNS double precision,
  NSBH double precision,
  BBH double precision,
  Terrestrial double precision
);

CREATE TABLE external_coincidence(
  superevent_id text PRIMARY KEY REFERENCES lvk(superevent_id),
  gcn_notice_id bigint NOT NULL,
  ivorn text NOT NULL,
  observatory text NOT NULL,
  search text NOT NULL,
  time_difference double precision NOT NULL,
  time_coincidence_far double precision NOT NULL,
  time_sky_position_coincidence_far double precision NOT NULL
);
