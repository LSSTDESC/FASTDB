-- This table is for keeping track of alerts that have been sent
--    in a simulation that sends alerts from the PPDB

CREATE TABLE ppdb_alerts_sent(
  id UUID PRIMARY KEY NOT NULL DEFAULT gen_random_uuid(),
  diasourceid bigint NOT NULL,
  senttime timestamp with time zone
);
CREATE INDEX idx_ppdb_alerts_sent_diasourceid ON ppdb_alerts_sent(diasourceid);
ALTER TABLE ppdb_alerts_sent ADD CONSTRAINT fk_ppdb_alerts_sent_diasource
  FOREIGN KEY (diasourceid) REFERENCES ppdb_diasource(diasourceid) ON DELETE CASCADE;
