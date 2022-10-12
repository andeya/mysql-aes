package aesdb

import (
	"context"
	"database/sql"

	"github.com/andeya/gust"
	"github.com/andeya/mysql-aes/modifier"
)

type AesTx struct {
	*sql.Tx
	aes gust.Option[*modifier.AesConverter]
}

func NewAesTx(tx *sql.Tx) gust.Result[AesTx] {
	dbName, err := modifier.QueryDbName(tx)
	if err != nil {
		return gust.Err[AesTx](err)
	}
	return gust.Ok(AesTx{Tx: tx, aes: modifier.NewAesConverter(dbName)})
}

func (tx AesTx) convertDml(query string) (string, error) {
	if tx.aes.IsNone() {
		return query, nil
	}
	return tx.aes.UnwrapUnchecked().ConvertDml(query)
}

func (tx AesTx) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	query, err := tx.convertDml(query)
	if err != nil {
		return nil, err
	}
	return tx.Tx.PrepareContext(ctx, query)
}

func (tx AesTx) Prepare(query string) (*sql.Stmt, error) {
	query, err := tx.convertDml(query)
	if err != nil {
		return nil, err
	}
	return tx.Tx.Prepare(query)
}

func (tx AesTx) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	// TODO: Direct encryption parameters
	query, err := tx.convertDml(query)
	if err != nil {
		return nil, err
	}
	return tx.Tx.ExecContext(ctx, query, args...)
}

func (tx AesTx) Exec(query string, args ...any) (sql.Result, error) {
	// TODO: Direct encryption parameters
	query, err := tx.convertDml(query)
	if err != nil {
		return nil, err
	}
	return tx.Tx.Exec(query, args...)
}

func (tx AesTx) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	query, err := tx.convertDml(query)
	if err != nil {
		return nil, err
	}
	return tx.Tx.QueryContext(ctx, query, args...)
}

func (tx AesTx) Query(query string, args ...any) (*sql.Rows, error) {
	query, err := tx.convertDml(query)
	if err != nil {
		return nil, err
	}
	return tx.Tx.Query(query, args...)
}

func (tx AesTx) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	query, err := tx.convertDml(query)
	if err != nil {
		return newRow(err)
	}
	return tx.Tx.QueryRowContext(ctx, query, args...)
}

func (tx AesTx) QueryRow(query string, args ...any) *sql.Row {
	query, err := tx.convertDml(query)
	if err != nil {
		return newRow(err)
	}
	return tx.Tx.QueryRow(query, args...)
}
