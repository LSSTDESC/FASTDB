ALTER TABLE diasource_brokerinfo
  ADD COLUMN msgtime TIMESTAMP WITH TIME ZONE DEFAULT NULL;
ALTER TABLE diasource_brokerinfo
  ADD COLUMN receivedtime TIMESTAMP WITH TIME ZONE DEFAULT NULL;
ALTER TABLE diasource_brokerinfo
  ADD COLUMN importtime TIMESTAMP WITH TIME ZONE DEFAULT NOW();
ALTER TABLE diasource_brokerinfo
  ADD COLUMN topic text DEFAULT '(none)';
ALTER TABLE diasource_brokerinfo DROP CONSTRAINT pk_diasource_brokerinfo;
ALTER TABLE diasource_brokerinfo ADD CONSTRAINT pk_diasoure_brokerinfo
  PRIMARY KEY (brokername, topic, diasourceid, base_procver_id);
