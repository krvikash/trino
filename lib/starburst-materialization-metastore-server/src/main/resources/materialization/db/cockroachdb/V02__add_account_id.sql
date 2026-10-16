-- NOT NULL from the start: Galaxy, the only CockroachDB deployment, had no rows before account ids
ALTER TABLE materializations ADD COLUMN account_id VARCHAR(32) NOT NULL;
