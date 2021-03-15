/*
------------------------------------------------------------------------------------------------------------------------
####### pgsql ####### (c) 2020-2021 mls-361 ######################################################## MIT License #######
------------------------------------------------------------------------------------------------------------------------
*/

package pgsql

import (
	"context"

	"github.com/jackc/pgx/v4"
)

type (
	// Transaction AFAIRE.
	Transaction struct {
		ctx context.Context
		tx  pgx.Tx
	}
)

// Execute AFAIRE.
func (t *Transaction) Execute(sql string, args ...interface{}) (int64, error) {
	result, err := t.tx.Exec(t.ctx, sql, args...)
	if err != nil {
		return 0, err
	}

	return result.RowsAffected(), nil
}

// Query AFAIRE.
func (t *Transaction) Query(sql string, args ...interface{}) (Rows, error) {
	return t.tx.Query(t.ctx, sql, args...)
}

// QueryRow AFAIRE.
func (t *Transaction) QueryRow(sql string, args ...interface{}) Row {
	return t.tx.QueryRow(t.ctx, sql, args...)
}

/*
######################################################################################################## @(°_°)@ #######
*/
