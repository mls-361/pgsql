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
	// BatchResults AFAIRE.
	BatchResults interface {
		Execute() (int64, error)
		Query() (Rows, error)
		QueryRow() Row
		Close() error
	}

	batchResults struct {
		results pgx.BatchResults
	}

	// Batch AFAIRE.
	Batch interface {
		Len() int
		Queue(sql string, args ...interface{})
		Send(ctx context.Context, client *Client) BatchResults
	}

	batch struct {
		*pgx.Batch
	}
)

func NewBatch() Batch {
	return &batch{
		Batch: &pgx.Batch{},
	}
}

func (b *batch) Send(ctx context.Context, client *Client) BatchResults {
	results := client.pool.SendBatch(ctx, b.Batch)

	return &batchResults{
		results: results,
	}
}

func (br *batchResults) Execute() (int64, error) {
	result, err := br.results.Exec()
	if err != nil {
		return 0, err
	}

	return result.RowsAffected(), nil
}

// Query AFAIRE.
func (br *batchResults) Query() (Rows, error) {
	return br.results.Query()
}

// QueryRow AFAIRE.
func (br *batchResults) QueryRow() Row {
	return br.results.QueryRow()
}

func (br *batchResults) Close() error {
	return br.results.Close()
}

/*
######################################################################################################## @(°_°)@ #######
*/
