package main

import (
	"context"
	"encoding/json"
	"math/big"

	"github.com/devmalloni/peluciopg"

	"github.com/devmalloni/pelucio"
	"github.com/golang-migrate/migrate/v4/database/postgres"
)

func main() {
	readWriter, err := peluciopg.NewReadWriterPG(context.Background(), "postgres://postgres:postgres@localhost:5432/pelucio_db?sslmode=disable")
	if err != nil {
		panic(err)
	}

	err = readWriter.Migrate(nil, "pelucio_db", &postgres.Config{})
	if err != nil {
		panic(err)
	}

	pelucioInstance := pelucio.NewPelucio(pelucio.WithReadWriter(readWriter))

	debitAccount, err := pelucioInstance.CreateAccount(context.Background(), "example-account", "Example Account", pelucio.Debit, json.RawMessage(`{"foo":"bar"}`))
	if err == pelucio.ErrExternalIDAlreadyInUse {
		debitAccount, _ = pelucioInstance.FindAccountByExternalID(context.TODO(), "example-account")
	}
	if err != nil && err != pelucio.ErrExternalIDAlreadyInUse {
		panic(err)
	}

	creditAccount, err := pelucioInstance.CreateAccount(context.Background(), "example-credit-account", "Example Credit Account", pelucio.Credit, nil)
	if err == pelucio.ErrExternalIDAlreadyInUse {
		creditAccount, _ = pelucioInstance.FindAccountByExternalID(context.TODO(), "example-credit-account")
	}
	if err != nil && err != pelucio.ErrExternalIDAlreadyInUse {
		panic(err)
	}

	amount, _ := pelucio.FromString("100.20", 2)
	deposit := pelucio.Deposit("example-deposit",
		debitAccount.ID,
		creditAccount.ID,
		amount,
		pelucio.Currency("BRL"))

	err = pelucioInstance.ExecuteTransaction(context.Background(), deposit)
	if err != nil && err != pelucio.ErrExternalIDAlreadyInUse {
		panic(err)
	}

	debitBalance, err := pelucioInstance.BalanceOf(context.Background(), debitAccount.ID)
	if err != nil {
		panic(err)
	}

	creditBalance, err := pelucioInstance.BalanceOf(context.Background(), creditAccount.ID)
	if err != nil {
		panic(err)
	}

	if debitBalance.Get("BRL").Cmp(big.NewInt(10020)) != 0 {
		panic("Debit account balance mismatch")
	}

	if creditBalance.Get("BRL").Cmp(big.NewInt(10020)) != 0 {
		panic("Debit account balance mismatch")
	}
}
