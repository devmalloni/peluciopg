package peluciopg

import (
	"embed"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	_ "github.com/lib/pq"
)

//go:embed migrations/*.sql
var migrationsFolder embed.FS

func NilInt(i int) *int {
	return &i
}

func (p *ReadWriterPG) Migrate(forceVersion *int, databaseName string, config *postgres.Config) error {
	driver, err := postgres.WithInstance(p.DB.DB, config)
	if err != nil {
		return err
	}

	d, err := iofs.New(migrationsFolder, "migrations")
	if err != nil {
		return err
	}

	m, err := migrate.NewWithInstance(
		"iofs",
		d,
		databaseName,
		driver)
	if err != nil {
		return err
	}

	if forceVersion != nil {
		err = m.Force(*forceVersion)
	} else {
		err = m.Up()
	}
	if err == migrate.ErrNoChange {
		err = nil
	}
	if err != nil {
		return err
	}

	return err
}
