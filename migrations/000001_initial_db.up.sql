BEGIN;

CREATE TABLE accounts (
    "id" uuid NOT NULL, 
    PRIMARY KEY ("id"),
    "external_id" varchar(255) NOT NULL UNIQUE,
    "name" varchar(255) NOT NULL,
    "normal_side" varchar(10) NOT NULL,
    "balance" jsonb,
    "metadata" jsonb,
    "version" bigint NOT NULL,
    "created_at" timestamp NOT NULL,
    "updated_at" timestamp,
    "deleted_at" timestamp
);

CREATE TABLE transactions (
    "id" uuid NOT NULL, 
    PRIMARY KEY ("id"),
    "external_id" varchar(255) NOT NULL UNIQUE,
    "description" varchar(255) NOT NULL,
    "metadata" jsonb,
    "created_at" timestamp NOT NULL
);

CREATE TABLE entries (
    "id" uuid NOT NULL, 
    PRIMARY KEY ("id"),
    "transaction_id" uuid NOT NULL,
    "account_id" uuid NOT NULL,
    "entry_side" varchar(10) NOT NULL,
    "account_side" varchar(10) NOT NULL,
    "amount" text NOT NULL,
    "currency" varchar(32) NOT NULL,
    "created_at" timestamp NOT NULL,
    CONSTRAINT entries_transactions FOREIGN KEY (transaction_id) REFERENCES transactions (id) ON DELETE CASCADE,
    CONSTRAINT entries_accounts FOREIGN KEY (account_id) REFERENCES accounts (id) ON DELETE CASCADE
);

END;

