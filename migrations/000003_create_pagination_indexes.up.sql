BEGIN;

CREATE INDEX idx_transactions_createdat_id ON transactions (created_at, id);

CREATE INDEX idx_entries_createdat_id ON entries (created_at, id);

CREATE INDEX idx_accounts_createdat_id ON accounts (created_at, id);

END;

