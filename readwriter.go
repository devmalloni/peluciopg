package peluciopg

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"slices"
	"strings"
	"time"

	"github.com/devmalloni/pelucio"
	"github.com/gofrs/uuid/v5"
	"github.com/jmoiron/sqlx"
)

type NullBigInt struct {
	Amount *big.Int
	Valid  bool
}

func (n *NullBigInt) Scan(value interface{}) error {
	if value == nil {
		n.Amount = nil
		n.Valid = false
		return nil
	}
	str, ok := value.(string)
	if !ok {
		return fmt.Errorf("NullRawMessage: failed to unmarshal JSONB value: %v", value)
	}

	amount, ok := new(big.Int).SetString(str, 10)
	if !ok {
		return errors.New("NullBigInt: failed to parse big.Int from string")
	}
	n.Amount = amount
	n.Valid = true

	return nil
}

func (n NullBigInt) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}
	return n.Amount.String(), nil
}

// NullRawMessage represents a json.RawMessage that may be null.
type NullRawMessage struct {
	RawMessage json.RawMessage
	Valid      bool // Valid is true if JSON is not NULL
}

func (n *NullRawMessage) Scan(value interface{}) error {
	if value == nil {
		n.RawMessage = nil
		n.Valid = false
		return nil
	}
	bytes, ok := value.([]byte)
	if !ok {
		return fmt.Errorf("NullRawMessage: failed to unmarshal JSONB value: %v", value)
	}

	n.RawMessage = slices.Clone(bytes)
	n.Valid = true
	return nil
}

func (n NullRawMessage) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}
	return []byte(n.RawMessage), nil
}

type account struct {
	pelucio.Account
	Balance  NullRawMessage `db:"balance" json:"balance"`
	Metadata NullRawMessage `db:"metadata" json:"metadata"`
}

func newAccountFromPelucio(acc *pelucio.Account) *account {
	dbAccount := account{
		Account: *acc,
	}

	if acc.Balance != nil {
		if b, err := json.Marshal(acc.Balance); err == nil {
			dbAccount.Balance = NullRawMessage{RawMessage: b, Valid: true}
		} else {
			panic(err)
		}
	} else {
		dbAccount.Balance = NullRawMessage{Valid: false}
	}

	if acc.Metadata != nil {
		dbAccount.Metadata = NullRawMessage{RawMessage: acc.Metadata, Valid: true}
	} else {
		dbAccount.Metadata = NullRawMessage{Valid: false}
	}

	return &dbAccount
}

func (p *account) ToAccount() (*pelucio.Account, error) {
	if p.Balance.Valid {
		var balance pelucio.Balance
		err := json.Unmarshal(p.Balance.RawMessage, &balance)
		if err != nil {
			return nil, err
		}

		p.Account.Balance = balance
	}
	if p.Metadata.Valid {
		p.Account.Metadata = p.Metadata.RawMessage
	}

	return &p.Account, nil
}

type transaction struct {
	pelucio.Transaction
	Metadata NullRawMessage `db:"metadata" json:"metadata"`
	Entries  []*entry
}

func (p *transaction) ToTransaction() *pelucio.Transaction {
	if p.Metadata.Valid {
		p.Transaction.Metadata = p.Metadata.RawMessage
	}

	for _, e := range p.Entries {
		p.Transaction.Entries = append(p.Transaction.Entries, e.ToEntry())
	}

	return &p.Transaction
}

type entry struct {
	pelucio.Entry
	Amount NullBigInt `db:"amount"`
}

func (p *entry) ToEntry() *pelucio.Entry {
	if p.Amount.Valid {
		p.Entry.Amount = p.Amount.Amount
	}
	return &p.Entry
}

func newEntryFromPelucio(entr *pelucio.Entry) *entry {
	db := entry{
		Entry: *entr,
	}

	if entr.Amount != nil {
		db.Amount = NullBigInt{Amount: entr.Amount, Valid: true}
	} else {
		db.Amount = NullBigInt{Valid: false}
	}

	return &db
}

func newTransactionFromPelucio(tr *pelucio.Transaction) *transaction {
	db := transaction{
		Transaction: *tr,
	}

	if tr.Metadata != nil {
		db.Metadata = NullRawMessage{RawMessage: tr.Metadata, Valid: true}
	} else {
		db.Metadata = NullRawMessage{Valid: false}
	}

	for _, e := range tr.Entries {
		db.Entries = append(db.Entries, newEntryFromPelucio(e))
	}

	return &db
}

type ReadWriterPG struct {
	DB *sqlx.DB
}

func NewReadWriterPG(ctx context.Context, dsn string) (*ReadWriterPG, error) {
	db, err := sqlx.ConnectContext(ctx, "postgres", dsn)
	if err != nil {
		return nil, err
	}

	p := &ReadWriterPG{
		DB: db,
	}

	return p, nil
}

func (rw *ReadWriterPG) WriteAccount(ctx context.Context, account *pelucio.Account, allowUpdate bool) error {
	if allowUpdate {
		return rw.upsertAccount(ctx, account)
	}

	return rw.insertAccount(ctx, account)
}

func (rw *ReadWriterPG) upsertAccount(ctx context.Context, account *pelucio.Account) error {
	dbAccount := newAccountFromPelucio(account)
	dbAccount.Version = time.Now().UnixNano()
	_, err := rw.DB.NamedExecContext(ctx, `
			INSERT INTO accounts (id, external_id, name, metadata, normal_side, version, balance, created_at, updated_at, deleted_at)
			VALUES (:id, :external_id :name, :metadata, :normal_side, :new_version, :balance, :created_at, :updated_at, :deleted_at)
			ON CONFLICT (id) DO UPDATE SET
				name       = EXCLUDED.name,
				metadata   = EXCLUDED.metadata,
				version    = EXCLUDED.version,
				created_at = EXCLUDED.created_at,
				updated_at = EXCLUDED.updated_at,
				deleted_at = EXCLUDED.deleted_at
			WHERE version = :version
		`, map[string]interface{}{
		"id":          dbAccount.ID,
		"external_id": dbAccount.ExternalID,
		"name":        dbAccount.Name,
		"metadata":    dbAccount.Metadata,
		"normal_side": dbAccount.NormalSide,
		"version":     account.Version,
		"balance":     dbAccount.Balance,
		"new_version": dbAccount.Version,
		"created_at":  dbAccount.CreatedAt,
		"updated_at":  dbAccount.UpdatedAt,
		"deleted_at":  dbAccount.DeletedAt,
	})

	return err
}

func (rw *ReadWriterPG) insertAccount(ctx context.Context, account *pelucio.Account) error {
	dbAccount := newAccountFromPelucio(account)

	dbAccount.Version = time.Now().UnixNano()
	_, err := rw.DB.NamedExecContext(ctx, `
			INSERT INTO accounts (id, external_id, balance, name, normal_side, metadata, version, created_at)
			VALUES (:id, :external_id, :balance, :name, :normal_side, :metadata, :version, :created_at)
		`, dbAccount)

	return err
}

func (rw *ReadWriterPG) WriteTransaction(ctx context.Context, transaction *pelucio.Transaction, accounts ...*pelucio.Account) error {
	tx, err := rw.DB.BeginTxx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	dbTransaction := newTransactionFromPelucio(transaction)
	_, err = tx.NamedExecContext(ctx, `
		INSERT INTO transactions (id, external_id, description, metadata, created_at)
		VALUES (:id, :external_id, :description, :metadata, :created_at)
	`, dbTransaction)
	if err != nil {
		return err
	}

	_, err = tx.NamedExecContext(ctx, `
			INSERT INTO entries (id, transaction_id, account_id, entry_side, account_side, amount, currency, created_at)
			VALUES (:id, :transaction_id, :account_id, :entry_side, :account_side, :amount, :currency, :created_at)		
	`, dbTransaction.Entries)
	if err != nil {
		return err
	}

	for _, acc := range accounts {
		account := newAccountFromPelucio(acc)
		m := map[string]interface{}{
			"id":          account.ID,
			"balance":     account.Balance,
			"version":     account.Version,
			"updated_at":  account.UpdatedAt,
			"new_version": time.Now().UnixNano(),
		}
		res, err := tx.NamedExecContext(ctx, `
			UPDATE accounts SET balance = :balance, 
								version = :new_version, 
								updated_at = :updated_at 
			WHERE id = :id AND version = :version
		`, m)
		if err != nil {
			return err
		}

		if rowsAffected, err := res.RowsAffected(); rowsAffected != 1 || err != nil {
			return pelucio.ErrNotFound
		}
	}

	return tx.Commit()
}

func (rw *ReadWriterPG) ReadAccount(ctx context.Context, accountID uuid.UUID) (*pelucio.Account, error) {
	var account account
	err := rw.DB.GetContext(ctx, &account, "SELECT * FROM accounts WHERE id = $1", accountID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, pelucio.ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	acc, err := account.ToAccount()
	if err != nil {
		return nil, err
	}

	return acc, nil
}

func (rw *ReadWriterPG) ReadAccountByExternalID(ctx context.Context, externalID string) (*pelucio.Account, error) {
	var account account
	err := rw.DB.GetContext(ctx, &account, "SELECT * FROM accounts WHERE external_id = $1", externalID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, pelucio.ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	acc, err := account.ToAccount()
	if err != nil {
		return nil, err
	}

	return acc, nil
}

func (rw *ReadWriterPG) ReadAccounts(ctx context.Context, filter pelucio.ReadAccountFilter) ([]*pelucio.Account, error) {
	conditions := []string{}
	args := []interface{}{}

	if filter.FromDate != nil {
		conditions = append(conditions, "created_at >= ?")
		args = append(args, filter.FromDate)
	}
	if filter.ToDate != nil {
		conditions = append(conditions, "created_at <= ?")
		args = append(args, filter.ToDate)
	}
	if filter.AccountIDs != nil {
		q, argss, _ := sqlx.In("id IN (?)", filter.AccountIDs)
		conditions = append(conditions, q)
		args = append(args, argss...)
	}
	if filter.ExternalIDs != nil {
		q, argss, _ := sqlx.In("external_id IN (?)", filter.ExternalIDs)
		conditions = append(conditions, q)
		args = append(args, argss...)
	}

	query := "SELECT * FROM accounts "
	if len(conditions) > 0 {
		query += " WHERE " + strings.Join(conditions, " AND ")
	}

	query += " ORDER BY created_at DESC "
	query = rw.DB.Rebind(query)

	accounts := []*account{}
	err := rw.DB.SelectContext(ctx, &accounts, query, args...)
	if err != nil {
		return nil, err
	}

	var res []*pelucio.Account
	for _, acc := range accounts {
		a, err := acc.ToAccount()
		if err != nil {
			return nil, err
		}
		res = append(res, a)
	}

	return res, nil
}

func (rw *ReadWriterPG) ReadTransaction(ctx context.Context, transactionID uuid.UUID) (*pelucio.Transaction, error) {
	var transaction transaction
	err := rw.DB.GetContext(ctx, &transaction, "SELECT * FROM transactions WHERE id = $1", transactionID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, pelucio.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return transaction.ToTransaction(), nil
}

func (rw *ReadWriterPG) ReadTransactionByExternalID(ctx context.Context, externalID string) (*pelucio.Transaction, error) {
	var transaction transaction
	err := rw.DB.GetContext(ctx, &transaction, "SELECT * FROM transactions WHERE external_id = $1", externalID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, pelucio.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return transaction.ToTransaction(), nil
}

func (rw *ReadWriterPG) ReadTransactions(ctx context.Context, filter pelucio.ReadTransactionFilter) ([]*pelucio.Transaction, error) {
	conditions := []string{}
	args := []interface{}{}
	if filter.FromDate != nil {
		conditions = append(conditions, "transactions.created_at >= ?")
		args = append(args, filter.FromDate)
	}
	if filter.ToDate != nil {
		conditions = append(conditions, "created_at <= ?")
		args = append(args, filter.ToDate)
	}
	if filter.AccountIDs != nil {
		q, argss, _ := sqlx.In("entries.account_id IN (?)", filter.AccountIDs)
		conditions = append(conditions, q)
		args = append(args, argss...)
	}
	if filter.ExternalIDs != nil {
		q, argss, _ := sqlx.In("transactions.external_id IN (?)", filter.ExternalIDs)
		conditions = append(conditions, q)
		args = append(args, argss...)
	}
	query := "SELECT transactions.* FROM transactions LEFT JOIN entries ON transactions.id = entries.transaction_id "
	if len(conditions) > 0 {
		query += " WHERE " + strings.Join(conditions, " AND ")
	}
	query += " ORDER BY transactions.created_at DESC"

	query = rw.DB.Rebind(query)

	transactionsDB := []*transaction{}
	err := rw.DB.SelectContext(ctx, &transactionsDB, query, args...)
	if err != nil {
		return nil, err
	}

	transactions := make([]*pelucio.Transaction, len(transactionsDB))
	for i, t := range transactionsDB {
		transactions[i] = t.ToTransaction()
	}

	return transactions, err
}

func (rw *ReadWriterPG) ReadEntriesOfAccount(ctx context.Context, accountID uuid.UUID) ([]*pelucio.Entry, error) {
	entriesdb := []*entry{}
	err := rw.DB.SelectContext(ctx, &entriesdb, "SELECT * FROM entries WHERE account_id = $1", accountID)
	if err != nil {
		return nil, err
	}

	entries := make([]*pelucio.Entry, len(entriesdb))
	for i, e := range entriesdb {
		entries[i] = e.ToEntry()
	}

	return entries, err
}

func (rw *ReadWriterPG) ReadEntries(ctx context.Context, filter pelucio.ReadEntryFilter) ([]*pelucio.Entry, error) {
	conditions := []string{}
	args := []interface{}{}
	if filter.FromDate != nil {
		conditions = append(conditions, "created_at >= ?")
		args = append(args, filter.FromDate)
	}
	if filter.ToDate != nil {
		conditions = append(conditions, "created_at <= ?")
		args = append(args, filter.ToDate)
	}
	if filter.AccountIDs != nil {
		query, argss, _ := sqlx.In("account_id IN ANY(?)", filter.AccountIDs)
		conditions = append(conditions, query)
		args = append(args, argss...)
	}
	if filter.TransactionIDs != nil {
		query, argss, _ := sqlx.In("transaction_id IN ANY(?)", filter.TransactionIDs)
		conditions = append(conditions, query)
		args = append(args, argss...)
	}
	query := "SELECT * FROM entries "
	if len(conditions) > 0 {
		query += " WHERE " + strings.Join(conditions, " AND ")
	}

	query += " ORDER BY created_at DESC"

	entriesdb := []*entry{}
	err := rw.DB.SelectContext(ctx, &entriesdb, query, args...)
	if err != nil {
		return nil, err
	}

	entries := make([]*pelucio.Entry, len(entriesdb))
	for i, e := range entriesdb {
		entries[i] = e.ToEntry()
	}

	return entries, err
}

func (rw *ReadWriterPG) ReadEntriesOfTransaction(ctx context.Context, transactionID uuid.UUID) ([]*pelucio.Entry, error) {
	entriesdb := []*entry{}
	err := rw.DB.SelectContext(ctx, &entriesdb, "SELECT * FROM entries WHERE transaction_id = $1", transactionID)
	if err != nil {
		return nil, err
	}

	entries := make([]*pelucio.Entry, len(entriesdb))
	for i, e := range entriesdb {
		entries[i] = e.ToEntry()
	}

	return entries, err
}
