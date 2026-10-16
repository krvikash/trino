-- Nullable while servers predating account ids may still write rows; V03 backfills existing ones
ALTER TABLE materializations ADD account_id VARCHAR(32)
