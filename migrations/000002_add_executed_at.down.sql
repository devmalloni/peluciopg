BEGIN;

ALTER TABLE transactions DROP COLUMN executed_at;

END;