/*
------------------------------------------------------------------------------------------------------------------------
####### pgsql ####### (c) 2020-2021 mls-361 ######################################################## MIT License #######
------------------------------------------------------------------------------------------------------------------------
*/

package pgsql

import (
	"context"

	"github.com/jackc/pgx/v4"
	"github.com/mls-361/failure"
)

type (
	// Transaction AFAIRE.
	Transaction struct {
		ctx context.Context
		tx  pgx.Tx
	}
)

// Transaction AFAIRE.
func (c *Client) Transaction(ctx context.Context, fn func(*Transaction) error) error {
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return err
	}

	t := &Transaction{
		ctx: ctx,
		tx:  tx,
	}

	if err := fn(t); err != nil {
		if rbErr := tx.Rollback(ctx); rbErr != nil {
			return failure.New(err).
				Set("reason", rbErr).
				Msg("transaction rollback error") //////////////////////////////////////////////////////////////////////
		}

		return err
	}

	return tx.Commit(ctx)
}

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
