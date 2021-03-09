/*
------------------------------------------------------------------------------------------------------------------------
####### pgsql ####### (c) 2020-2021 mls-361 ######################################################## MIT License #######
------------------------------------------------------------------------------------------------------------------------
*/

package pgsql

import (
	"context"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/mls-361/logger"
)

var (
	// ErrNoRows AFAIRE.
	ErrNoRows = pgx.ErrNoRows
)

type (
	// Row AFAIRE.
	Row interface {
		Scan(dest ...interface{}) error
	}

	// Rows AFAIRE.
	Rows interface {
		Next() bool
		Scan(dest ...interface{}) error
		Close()
		Err() error
	}

	// Client AFAIRE.
	Client struct {
		logger *pgxLogger
		pool   *pgxpool.Pool
	}
)

// NewClient AFAIRE.
func NewClient(logger logger.Logger) *Client {
	var pgxl *pgxLogger

	if logger != nil {
		pgxl = &pgxLogger{
			Logger: logger,
		}
	}

	return &Client{
		logger: pgxl,
	}
}

// Connect AFAIRE.
func (c *Client) Connect(ctx context.Context, uri string) error {
	config, err := pgxpool.ParseConfig(uri)
	if err != nil {
		return err
	}

	if c.logger != nil {
		config.ConnConfig.LogLevel = pgx.LogLevelWarn
		config.ConnConfig.Logger = c.logger
	}

	pool, err := pgxpool.ConnectConfig(ctx, config)
	if err != nil {
		return err
	}

	c.pool = pool

	return nil
}

// ContextWT AFAIRE.
func (c *Client) ContextWT(timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), timeout)
}

// Execute AFAIRE.
func (c *Client) Execute(ctx context.Context, sql string, args ...interface{}) (int64, error) {
	result, err := c.pool.Exec(ctx, sql, args...)
	if err != nil {
		return 0, err
	}

	return result.RowsAffected(), nil
}

// Query AFAIRE.
func (c *Client) Query(ctx context.Context, sql string, args ...interface{}) (Rows, error) {
	return c.pool.Query(ctx, sql, args...)
}

// QueryRow AFAIRE.
func (c *Client) QueryRow(ctx context.Context, sql string, args ...interface{}) Row {
	return c.pool.QueryRow(ctx, sql, args...)
}

// Close AFAIRE.
func (c *Client) Close() {
	if c.pool != nil {
		c.pool.Close() // AFINIR: doesn't give back the hand if the database is stopped and then restarted!
		c.pool = nil
	}
}

/*
######################################################################################################## @(°_°)@ #######
*/
