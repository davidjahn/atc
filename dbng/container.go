package dbng

import (
	sq "github.com/Masterminds/squirrel"
	"database/sql"
)

type ContainerState string

const (
	ContainerStateCreating   = "creating"
	ContainerStateCreated    = "created"
	ContainerStateDestroying = "destroying"
)

type CreatingContainer struct {
	ID   int
	conn Conn
}

type CreatedContainer struct {
	ID   int
	conn Conn
}

func (container *CreatedContainer) Destroying() (*DestroyingContainer, error) {
	tx, err := container.conn.Begin()
	if err != nil {
		return nil, err
	}

	defer tx.Rollback()

	var (
		workerName string
		handle     sql.NullString
	)

	err = psql.Update("containers").
		Set("state", ContainerStateDestroying).
		Where(sq.Eq{
			"id":    container.ID,
			"state": ContainerStateCreated,
		}).
		Suffix("RETURNING worker_name, handle").
		RunWith(tx).
		QueryRow().
		Scan(&workerName, &handle)

	if err != nil {
		return nil, err
	}

	err = tx.Commit()
	if err != nil {
		return nil, err
	}

	return &DestroyingContainer{
		ID:         container.ID,
		WorkerName: workerName,
		Handle:     handle,
		conn:       container.conn,
	}, nil
}

type DestroyingContainer struct {
	ID         int
	WorkerName string
	Handle     *string
	conn       Conn
}

func (container *DestroyingContainer) Destroy() (bool, error) {
	tx, err := container.conn.Begin()
	if err != nil {
		return false, err
	}

	defer tx.Rollback()

	rows, err := psql.Delete("containers").
		Where(sq.Eq{
			"id":    container.ID,
			"state": ContainerStateDestroying,
		}).
		RunWith(tx).
		Exec()
	if err != nil {
		return false, err
	}

	err = tx.Commit()
	if err != nil {
		return false, err
	}

	affected, err := rows.RowsAffected()
	if err != nil {
		return false, err
	}

	if affected == 0 {
		panic("TESTME")
		return false, nil
	}

	return true, nil
}
