BEGIN;

ALTER TABLE transactions ADD COLUMN executed_at timestamp;

UPDATE transactions SET executed_at = created_at;

ALTER TABLE transactions ALTER COLUMN executed_at SET NOT NULL;

END;

