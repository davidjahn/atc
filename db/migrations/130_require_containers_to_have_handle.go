package migrations

import "github.com/BurntSushi/migration"

func RequireContainersToHaveHandle(tx migration.LimitedTx) error {
	_, err := tx.Exec(`
		ALTER TABLE containers
    DROP CONSTRAINT handle_when_created,
    ALTER COLUMN handle SET NOT NULL
    ;
	`)
	if err != nil {
		return err
	}

	return nil
}
