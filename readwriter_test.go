package peluciopg

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/devmalloni/pelucio"
	"github.com/devmalloni/pelucio/x/xtime"
	"github.com/devmalloni/pelucio/x/xuuid"
	"github.com/gofrs/uuid/v5"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
)

func setupMockDB(t *testing.T) (*ReadWriterPG, sqlmock.Sqlmock, func()) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to open sqlmock database: %v", err)
	}
	sqlxdb := sqlx.NewDb(db, "postgres")
	return &ReadWriterPG{
		DB: sqlxdb,
	}, mock, func() { db.Close() }
}

func TestWriteAccount_Insert(t *testing.T) {
	db, mock, cleanup := setupMockDB(t)
	defer cleanup()

	acc := &pelucio.Account{
		ID:         uuid.Must(uuid.NewV4()),
		ExternalID: "extid",
		NormalSide: pelucio.Debit,
		Name:       "test",
		CreatedAt:  time.Now(),
	}

	mock.ExpectExec("INSERT INTO accounts").
		WithArgs(acc.ID, acc.ExternalID, sqlmock.AnyArg(), acc.Name, acc.NormalSide, sqlmock.AnyArg(), sqlmock.AnyArg(), acc.CreatedAt).
		WillReturnResult(sqlmock.NewResult(1, 1))

	err := db.WriteAccount(context.Background(), acc, false)
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestWriteAccount_Upsert(t *testing.T) {
	db, mock, cleanup := setupMockDB(t)
	defer cleanup()

	acc := &pelucio.Account{
		ID:         uuid.Must(uuid.NewV4()),
		ExternalID: "extid",
		NormalSide: pelucio.Debit,
		Name:       "test",
		CreatedAt:  time.Now(),
	}

	mock.ExpectExec("INSERT INTO accounts .* UPDATE SET").
		WithArgs(acc.ID,
			acc.ExternalID,
			acc.Name,
			sqlmock.AnyArg(), // metadata
			acc.NormalSide,
			sqlmock.AnyArg(), // version
			sqlmock.AnyArg(), // balance
			acc.CreatedAt,
			acc.UpdatedAt,
			acc.DeletedAt,
			sqlmock.AnyArg()). // new_version
		WillReturnResult(sqlmock.NewResult(1, 1))

	err := db.WriteAccount(context.Background(), acc, true)
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestWriteTransaction(t *testing.T) {
	db, mock, cleanup := setupMockDB(t)
	defer cleanup()

	firstAccount := &pelucio.Account{
		ID:         uuid.Must(uuid.NewV4()),
		ExternalID: "extid",
		NormalSide: pelucio.Debit,
		Name:       "test",
		CreatedAt:  time.Now(),
	}

	secondAccount := &pelucio.Account{
		ID:         uuid.Must(uuid.NewV4()),
		ExternalID: "extid",
		NormalSide: pelucio.Debit,
		Name:       "test",
		CreatedAt:  time.Now(),
	}

	transaction := pelucio.Deposit("external", firstAccount.ID, secondAccount.ID, big.NewInt(100), "USD")

	mock.ExpectBegin()

	mock.ExpectExec("INSERT INTO transactions").
		WithArgs(transaction.ID, transaction.ExternalID, transaction.Description, sqlmock.AnyArg(), transaction.CreatedAt). // new_version
		WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectExec("INSERT INTO entries").
		WithArgs(transaction.Entries[0].ID,
			transaction.Entries[0].TransactionID,
			transaction.Entries[0].AccountID,
			transaction.Entries[0].EntrySide,
			transaction.Entries[0].AccountSide,
			sqlmock.AnyArg(),
			transaction.Entries[0].Currency,
			transaction.Entries[0].CreatedAt,
			transaction.Entries[1].ID,
			transaction.Entries[1].TransactionID,
			transaction.Entries[1].AccountID,
			transaction.Entries[1].EntrySide,
			transaction.Entries[1].AccountSide,
			sqlmock.AnyArg(),
			transaction.Entries[1].Currency,
			transaction.Entries[1].CreatedAt).
		WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectExec("UPDATE accounts").
		WithArgs(
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			firstAccount.UpdatedAt,
			firstAccount.ID,
			firstAccount.Version,
		).
		WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectExec("UPDATE accounts").
		WithArgs(
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			secondAccount.UpdatedAt,
			secondAccount.ID,
			secondAccount.Version,
		).
		WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectCommit()

	err := db.WriteTransaction(context.Background(), transaction, firstAccount, secondAccount)
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())

}

func TestReadAccount(t *testing.T) {
	db, mock, cleanup := setupMockDB(t)
	defer cleanup()

	accID := uuid.Must(uuid.NewV4())
	acc := &pelucio.Account{
		ID:         accID,
		ExternalID: "extid",
		NormalSide: pelucio.Debit,
		Name:       "test",
		Balance:    map[pelucio.Currency]*big.Int{"BRL": big.NewInt(100)},
		CreatedAt:  time.Now(),
	}

	rows := sqlmock.
		NewRows([]string{"id", "external_id", "name", "metadata", "normal_side", "version", "balance", "created_at", "updated_at", "deleted_at"}).
		AddRow(acc.ID, acc.ExternalID, acc.Name, []byte("{}"), acc.NormalSide, int64(1), []byte("{\"BRL\": 100 }"), acc.CreatedAt, acc.UpdatedAt, acc.DeletedAt)

	mock.ExpectQuery("SELECT (.+) FROM accounts WHERE id = \\$1").
		WithArgs(accID).
		WillReturnRows(rows)

	resultAcc, err := db.ReadAccount(context.Background(), accID)
	assert.NoError(t, err)
	assert.Equal(t, acc.ID, resultAcc.ID)
	assert.Equal(t, acc.ExternalID, resultAcc.ExternalID)
	assert.Equal(t, acc.Name, resultAcc.Name)
	assert.Equal(t, acc.NormalSide, resultAcc.NormalSide)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestReadAccountByExternalID(t *testing.T) {
	db, mock, cleanup := setupMockDB(t)
	defer cleanup()

	accID := uuid.Must(uuid.NewV4())
	acc := &pelucio.Account{
		ID:         accID,
		ExternalID: "extid",
		NormalSide: pelucio.Debit,
		Name:       "test",
		CreatedAt:  time.Now(),
	}

	rows := sqlmock.
		NewRows([]string{"id", "external_id", "name", "metadata", "normal_side", "version", "balance", "created_at", "updated_at", "deleted_at"}).
		AddRow(acc.ID, acc.ExternalID, acc.Name, []byte("{}"), acc.NormalSide, int64(1), []byte("{\"BRL\": 100 }"), acc.CreatedAt, acc.UpdatedAt, acc.DeletedAt)

	mock.ExpectQuery("SELECT (.+) FROM accounts WHERE external_id = \\$1").
		WithArgs(acc.ExternalID).
		WillReturnRows(rows)

	resultAcc, err := db.ReadAccountByExternalID(context.Background(), acc.ExternalID)
	assert.NoError(t, err)
	assert.Equal(t, acc.ID, resultAcc.ID)
	assert.Equal(t, acc.ExternalID, resultAcc.ExternalID)
	assert.Equal(t, acc.Name, resultAcc.Name)
	assert.Equal(t, acc.NormalSide, resultAcc.NormalSide)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestReadAccounts(t *testing.T) {
	db, mock, cleanup := setupMockDB(t)
	defer cleanup()

	filter := pelucio.ReadAccountFilter{
		FromDate:    xtime.DefaultClock.NilNow(),
		ToDate:      xtime.DefaultClock.NilNow(),
		AccountIDs:  []string{xuuid.New().String()},
		ExternalIDs: []string{"external1", "external2"},
	}

	firstAccount := pelucio.NewAccount("external1", "First Account", pelucio.Debit, nil, xtime.DefaultClock)
	secondAccount := pelucio.NewAccount("external2", "Second Account", pelucio.Debit, nil, xtime.DefaultClock)

	txRows := sqlmock.
		NewRows([]string{"id", "external_id", "name", "metadata", "normal_side", "version", "balance", "created_at", "updated_at", "deleted_at"}).
		AddRow(firstAccount.ID, firstAccount.ExternalID, firstAccount.Name, []byte("{}"), firstAccount.NormalSide, int64(1), []byte("{\"BRL\": 100 }"), firstAccount.CreatedAt, firstAccount.UpdatedAt, firstAccount.DeletedAt).
		AddRow(secondAccount.ID, secondAccount.ExternalID, secondAccount.Name, []byte("{}"), secondAccount.NormalSide, int64(1), []byte("{\"BRL\": 100 }"), secondAccount.CreatedAt, secondAccount.UpdatedAt, secondAccount.DeletedAt)

	mock.ExpectQuery("SELECT (.+) FROM accounts (.+) ORDER BY created_at DESC").
		WithArgs(filter.FromDate, filter.ToDate, filter.AccountIDs[0], filter.ExternalIDs[0], filter.ExternalIDs[1]).
		WillReturnRows(txRows)

	resultTxs, err := db.ReadAccounts(context.Background(), filter)
	assert.NoError(t, err)
	assert.Len(t, resultTxs, 2)
	assert.Equal(t, firstAccount.ID, resultTxs[0].ID)
	assert.Equal(t, secondAccount.ID, resultTxs[1].ID)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestReadTransaction(t *testing.T) {
	db, mock, cleanup := setupMockDB(t)
	defer cleanup()

	transaction := pelucio.Deposit("external", xuuid.New(), xuuid.New(), big.NewInt(100), "USD")

	txRows := sqlmock.
		NewRows([]string{"id", "external_id", "description", "metadata", "created_at"}).
		AddRow(transaction.ID, transaction.ExternalID, transaction.Description, []byte("{}"), transaction.CreatedAt)

	mock.ExpectQuery("SELECT (.+) FROM transactions WHERE id = \\$1").
		WithArgs(transaction.ID).
		WillReturnRows(txRows)

	resultTx, err := db.ReadTransaction(context.Background(), transaction.ID)
	assert.NoError(t, err)
	assert.Equal(t, transaction.ID, resultTx.ID)
	assert.Equal(t, transaction.ExternalID, resultTx.ExternalID)
	assert.Equal(t, transaction.Description, resultTx.Description)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestReadTransactionByExternalID(t *testing.T) {
	db, mock, cleanup := setupMockDB(t)
	defer cleanup()

	transaction := pelucio.Deposit("external", xuuid.New(), xuuid.New(), big.NewInt(100), "USD")

	txRows := sqlmock.
		NewRows([]string{"id", "external_id", "description", "metadata", "created_at"}).
		AddRow(transaction.ID, transaction.ExternalID, transaction.Description, []byte("{}"), transaction.CreatedAt)

	mock.ExpectQuery("SELECT (.+) FROM transactions WHERE external_id = \\$1").
		WithArgs(transaction.ExternalID).
		WillReturnRows(txRows)

	resultTx, err := db.ReadTransactionByExternalID(context.Background(), transaction.ExternalID)
	assert.NoError(t, err)
	assert.Equal(t, transaction.ID, resultTx.ID)
	assert.Equal(t, transaction.ExternalID, resultTx.ExternalID)
	assert.Equal(t, transaction.Description, resultTx.Description)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestReadTransactions(t *testing.T) {
	db, mock, cleanup := setupMockDB(t)
	defer cleanup()

	filter := pelucio.ReadTransactionFilter{
		FromDate:    xtime.DefaultClock.NilNow(),
		ToDate:      xtime.DefaultClock.NilNow(),
		AccountIDs:  []string{xuuid.New().String()},
		ExternalIDs: []string{"external1", "external2"},
	}

	firstTx := pelucio.Deposit("external1", xuuid.New(), xuuid.New(), big.NewInt(100), "USD")
	secondTx := pelucio.Deposit("external2", xuuid.New(), xuuid.New(), big.NewInt(50), "USD")

	txRows := sqlmock.
		NewRows([]string{"id", "external_id", "description", "metadata", "created_at"}).
		AddRow(firstTx.ID, firstTx.ExternalID, firstTx.Description, []byte("{}"), firstTx.CreatedAt).
		AddRow(secondTx.ID, secondTx.ExternalID, secondTx.Description, []byte("{}"), secondTx.CreatedAt)

	mock.ExpectQuery("SELECT (.+) FROM transactions LEFT JOIN entries ON transactions.id = entries.transaction_id (.+) ORDER BY transactions.created_at DESC").
		WithArgs(filter.FromDate, filter.ToDate, filter.AccountIDs[0], filter.ExternalIDs[0], filter.ExternalIDs[1]).
		WillReturnRows(txRows)

	resultTxs, err := db.ReadTransactions(context.Background(), filter)
	assert.NoError(t, err)
	assert.Len(t, resultTxs, 2)
	assert.Equal(t, firstTx.ID, resultTxs[0].ID)
	assert.Equal(t, secondTx.ID, resultTxs[1].ID)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestReadEntriesOfAccount(t *testing.T) {
	db, mock, cleanup := setupMockDB(t)
	defer cleanup()

	accID := uuid.Must(uuid.NewV4())
	entr := &pelucio.Entry{
		ID:          xuuid.New(),
		EntrySide:   pelucio.Debit,
		AccountID:   accID,
		AccountSide: pelucio.Credit,
		Amount:      big.NewInt(100),
		Currency:    "USD",
		CreatedAt:   time.Now(),
	}

	rows := sqlmock.
		NewRows([]string{
			"id",
			"transaction_id",
			"account_id",
			"entry_side",
			"account_side",
			"amount",
			"currency",
			"created_at"}).
		AddRow(entr.ID, entr.TransactionID, entr.AccountID, entr.EntrySide, entr.AccountSide, "100", entr.Currency, entr.CreatedAt)

	mock.ExpectQuery("SELECT (.+) FROM entries WHERE account_id = \\$1").
		WithArgs(accID).
		WillReturnRows(rows)

	resultEntr, err := db.ReadEntriesOfAccount(context.Background(), accID)
	assert.NoError(t, err)
	assert.Equal(t, entr.ID, resultEntr[0].ID)
	assert.Equal(t, entr.AccountID, resultEntr[0].AccountID)
	assert.True(t, entr.Amount.Cmp(resultEntr[0].Amount) == 0)
	assert.Equal(t, entr.AccountSide, resultEntr[0].AccountSide)
	assert.Equal(t, entr.EntrySide, resultEntr[0].EntrySide)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestReadEntries(t *testing.T) {
	db, mock, cleanup := setupMockDB(t)
	defer cleanup()

	filter := pelucio.ReadEntryFilter{
		FromDate:       xtime.DefaultClock.NilNow(),
		ToDate:         xtime.DefaultClock.NilNow(),
		AccountIDs:     []string{xuuid.New().String()},
		TransactionIDs: []string{"external1", "external2"},
	}

	firstEntry := &pelucio.Entry{
		ID:          xuuid.New(),
		EntrySide:   pelucio.Debit,
		AccountID:   xuuid.New(),
		AccountSide: pelucio.Credit,
		Amount:      big.NewInt(100),
		Currency:    "USD",
		CreatedAt:   time.Now(),
	}
	secondEntry := &pelucio.Entry{
		ID:          xuuid.New(),
		EntrySide:   pelucio.Debit,
		AccountID:   xuuid.New(),
		AccountSide: pelucio.Credit,
		Amount:      big.NewInt(100),
		Currency:    "USD",
		CreatedAt:   time.Now(),
	}

	txRows := sqlmock.
		NewRows([]string{
			"id",
			"transaction_id",
			"account_id",
			"entry_side",
			"account_side",
			"amount",
			"currency",
			"created_at"}).
		AddRow(firstEntry.ID, firstEntry.TransactionID, firstEntry.AccountID, firstEntry.EntrySide, firstEntry.AccountSide, "100", firstEntry.Currency, firstEntry.CreatedAt).
		AddRow(secondEntry.ID, secondEntry.TransactionID, secondEntry.AccountID, secondEntry.EntrySide, secondEntry.AccountSide, "100", secondEntry.Currency, secondEntry.CreatedAt)

	mock.ExpectQuery("SELECT (.+) FROM entries (.+) ORDER BY created_at DESC").
		WithArgs(filter.FromDate, filter.ToDate, filter.AccountIDs[0], filter.TransactionIDs[0], filter.TransactionIDs[1]).
		WillReturnRows(txRows)

	resultTxs, err := db.ReadEntries(context.Background(), filter)
	assert.NoError(t, err)
	assert.Len(t, resultTxs, 2)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestReadEntriesOfTransaction(t *testing.T) {
	db, mock, cleanup := setupMockDB(t)
	defer cleanup()

	accID := uuid.Must(uuid.NewV4())
	entr := &pelucio.Entry{
		ID:            xuuid.New(),
		TransactionID: xuuid.New(),
		EntrySide:     pelucio.Debit,
		AccountID:     accID,
		AccountSide:   pelucio.Credit,
		Amount:        big.NewInt(100),
		Currency:      "USD",
		CreatedAt:     time.Now(),
	}

	rows := sqlmock.
		NewRows([]string{
			"id",
			"transaction_id",
			"account_id",
			"entry_side",
			"account_side",
			"amount",
			"currency",
			"created_at"}).
		AddRow(entr.ID, entr.TransactionID, entr.AccountID, entr.EntrySide, entr.AccountSide, "100", entr.Currency, entr.CreatedAt)

	mock.ExpectQuery("SELECT (.+) FROM entries WHERE transaction_id = \\$1").
		WithArgs(entr.TransactionID).
		WillReturnRows(rows)

	resultEntr, err := db.ReadEntriesOfTransaction(context.Background(), entr.TransactionID)
	assert.NoError(t, err)
	assert.Equal(t, entr.ID, resultEntr[0].ID)
	assert.Equal(t, entr.AccountID, resultEntr[0].AccountID)
	assert.True(t, entr.Amount.Cmp(resultEntr[0].Amount) == 0)
	assert.Equal(t, entr.AccountSide, resultEntr[0].AccountSide)
	assert.Equal(t, entr.EntrySide, resultEntr[0].EntrySide)
	assert.NoError(t, mock.ExpectationsWereMet())
}
