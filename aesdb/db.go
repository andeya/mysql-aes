package aesdb

import (
	"context"
	"database/sql"
	"reflect"

	"github.com/andeya/gust"
	"github.com/andeya/mysql-aes/modifier"
)

type AesDB struct {
	*sql.DB
	aes gust.Option[*modifier.AesConverter]
}

func NewAesDB(db *sql.DB) gust.Result[AesDB] {
	dbName, err := modifier.QueryDbName(db)
	if err != nil {
		return gust.Err[AesDB](err)
	}
	return gust.Ok(AesDB{DB: db, aes: modifier.NewAesConverter(dbName)})
}

func (db AesDB) convertDml(query string) (string, error) {
	if db.aes.IsNone() {
		return query, nil
	}
	return db.aes.UnwrapUnchecked().ConvertDml(query)
}

func (db AesDB) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	query, err := db.convertDml(query)
	if err != nil {
		return nil, err
	}
	return db.DB.PrepareContext(ctx, query)
}

func (db AesDB) Prepare(query string) (*sql.Stmt, error) {
	query, err := db.convertDml(query)
	if err != nil {
		return nil, err
	}
	return db.DB.Prepare(query)
}

func (db AesDB) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	// TODO: Direct encryption parameters
	query, err := db.convertDml(query)
	if err != nil {
		return nil, err
	}
	return db.DB.ExecContext(ctx, query, args...)
}

func (db AesDB) Exec(query string, args ...any) (sql.Result, error) {
	// TODO: Direct encryption parameters
	query, err := db.convertDml(query)
	if err != nil {
		return nil, err
	}
	return db.DB.Exec(query, args...)
}

func (db AesDB) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	query, err := db.convertDml(query)
	if err != nil {
		return nil, err
	}
	return db.DB.QueryContext(ctx, query, args...)
}

func (db AesDB) Query(query string, args ...any) (*sql.Rows, error) {
	query, err := db.convertDml(query)
	if err != nil {
		return nil, err
	}
	return db.DB.Query(query, args...)
}

func newRow(err error) *sql.Row {
	row := new(sql.Row)
	if err != nil {
		reflect.ValueOf(row).Elem().FieldByName("err").Set(reflect.ValueOf(err))
	}
	return row
}

func (db AesDB) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	query, err := db.convertDml(query)
	if err != nil {
		return newRow(err)
	}
	return db.DB.QueryRowContext(ctx, query, args...)
}

func (db AesDB) QueryRow(query string, args ...any) *sql.Row {
	query, err := db.convertDml(query)
	if err != nil {
		return newRow(err)
	}
	return db.DB.QueryRow(query, args...)
}

func (db AesDB) BeginTx(ctx context.Context, opts *sql.TxOptions) (AesTx, error) {
	tx, err := db.DB.BeginTx(ctx, opts)
	if err != nil {
		return AesTx{}, err
	}
	return AesTx{Tx: tx, aes: db.aes}, nil
}

func (db AesDB) Begin() (AesTx, error) {
	tx, err := db.DB.Begin()
	if err != nil {
		return AesTx{}, err
	}
	return AesTx{Tx: tx, aes: db.aes}, nil
}

func (db AesDB) Conn(ctx context.Context) (AesConn, error) {
	conn, err := db.DB.Conn(ctx)
	if err != nil {
		return AesConn{}, err
	}
	return AesConn{Conn: conn, aes: db.aes}, nil
}
