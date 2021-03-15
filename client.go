/*
------------------------------------------------------------------------------------------------------------------------
####### pgsql ####### (c) 2020-2021 mls-361 ######################################################## MIT License #######
------------------------------------------------------------------------------------------------------------------------
*/

package pgsql

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/mls-361/failure"
	"github.com/mls-361/logger"
)

const (
	_defaultConnectTimeout = 5 * time.Second
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
		config *Config
		logger *pgxLogger
		pool   *pgxpool.Pool
	}
)

// Context AFAIRE.
func Context(timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), timeout)
}

func newClient(cfg *Config, logger logger.Logger) *Client {
	var pgxl *pgxLogger

	if logger != nil {
		pgxl = &pgxLogger{
			Logger: logger,
		}
	}

	return &Client{
		config: cfg,
		logger: pgxl,
	}
}

// Database AFAIRE.
func (cl *Client) Database() string {
	return cl.config.Database
}

// Pool AFAIRE.
func (cl *Client) Pool() *pgxpool.Pool {
	return cl.pool
}

// Connect AFAIRE.
func (cl *Client) Connect() error {
	uri := fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s",
		cl.config.Username,
		cl.config.Password,
		cl.config.Host,
		cl.config.Port,
		cl.config.Database,
	)

	config, err := pgxpool.ParseConfig(uri)
	if err != nil {
		return err
	}

	config.MaxConnLifetime = cl.config.ConnLifeTime
	config.MaxConnIdleTime = cl.config.ConnIdleTime
	config.MaxConns = cl.config.MaxConns
	config.MinConns = cl.config.MinConns
	config.HealthCheckPeriod = cl.config.HealthCheckPeriod

	if cl.logger != nil {
		config.ConnConfig.LogLevel = pgx.LogLevelWarn
		config.ConnConfig.Logger = cl.logger
	}

	timeout := cl.config.ConnectTimeout

	if timeout == 0 {
		timeout = _defaultConnectTimeout
	}

	ctx, cancel := Context(timeout)
	defer cancel()

	pool, err := pgxpool.ConnectConfig(ctx, config)
	if err != nil {
		return err
	}

	cl.pool = pool

	return nil
}

// Execute AFAIRE.
func (cl *Client) Execute(ctx context.Context, sql string, args ...interface{}) (int64, error) {
	result, err := cl.pool.Exec(ctx, sql, args...)
	if err != nil {
		return 0, err
	}

	return result.RowsAffected(), nil
}

// Query AFAIRE.
func (cl *Client) Query(ctx context.Context, sql string, args ...interface{}) (Rows, error) {
	return cl.pool.Query(ctx, sql, args...)
}

// QueryRow AFAIRE.
func (cl *Client) QueryRow(ctx context.Context, sql string, args ...interface{}) Row {
	return cl.pool.QueryRow(ctx, sql, args...)
}

// Close AFAIRE.
func (cl *Client) Close() {
	if cl.pool != nil {
		cl.pool.Close()
		cl.pool = nil
	}
}

// Transaction AFAIRE.
func (cl *Client) Transaction(ctx context.Context, fn func(*Transaction) error) error {
	tx, err := cl.pool.Begin(ctx)
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

/*
######################################################################################################## @(°_°)@ #######
*/
