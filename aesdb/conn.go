package aesdb

import (
	"context"
	"database/sql"

	"github.com/andeya/gust"
	"github.com/andeya/mysql-aes/modifier"
)

type AesConn struct {
	*sql.Conn
	aes gust.Option[*modifier.AesConverter]
}

func NewAesConn(conn *sql.Conn) gust.Result[AesConn] {
	dbName, err := modifier.QueryDbName(conn)
	if err != nil {
		return gust.Err[AesConn](err)
	}
	return gust.Ok(AesConn{Conn: conn, aes: modifier.NewAesConverter(dbName)})
}

func (conn AesConn) convertDml(query string) (string, error) {
	if conn.aes.IsNone() {
		return query, nil
	}
	return conn.aes.UnwrapUnchecked().ConvertDml(query)
}

func (conn AesConn) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	// TODO: Direct encryption parameters
	query, err := conn.convertDml(query)
	if err != nil {
		return nil, err
	}
	return conn.Conn.ExecContext(ctx, query, args...)
}

func (conn AesConn) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	query, err := conn.convertDml(query)
	if err != nil {
		return nil, err
	}
	return conn.Conn.QueryContext(ctx, query, args...)
}

func (conn AesConn) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	query, err := conn.convertDml(query)
	if err != nil {
		return newRow(err)
	}
	return conn.Conn.QueryRowContext(ctx, query, args...)
}

func (conn AesConn) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	query, err := conn.convertDml(query)
	if err != nil {
		return nil, err
	}
	return conn.Conn.PrepareContext(ctx, query)
}

func (conn AesConn) BeginTx(ctx context.Context, opts *sql.TxOptions) (AesTx, error) {
	tx, err := conn.Conn.BeginTx(ctx, opts)
	if err != nil {
		return AesTx{}, err
	}
	return AesTx{Tx: tx, aes: conn.aes}, nil
}
