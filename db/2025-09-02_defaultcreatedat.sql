ALTER TABLE plannedspectra ALTER COLUMN created_at SET DEFAULT NOW();
ALTER TABLE wantedspectra ALTER COLUMN wanttime SET DEFAULT NOW();
ALTER TABLE spectruminfo ALTER COLUMN inserted_at SET DEFAULT NOW();
