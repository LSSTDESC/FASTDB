ALTER TABLE wantedspectra ADD COLUMN is_host boolean;
ALTER TABLE wantedspectra ADD COLUMN ra double precision;
ALTER TABLE wantedspectra ADD COLUMN dec double precision;
CREATE INDEX ix_wantedspectra_q3c ON wantedspectra( q3c_ang2ipix( ra, dec ) );
ALTER TABLE plannedspectra ADD COLUMN is_host boolean;
ALTER TABLE plannedspectra ADD COLUMN wantspec_id text;

