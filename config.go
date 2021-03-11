/*
------------------------------------------------------------------------------------------------------------------------
####### pgsql ####### (c) 2020-2021 mls-361 ######################################################## MIT License #######
------------------------------------------------------------------------------------------------------------------------
*/

package pgsql

import (
	"fmt"
	"time"

	"github.com/mls-361/logger"
)

const (
	_poolMaxConns   = 10
	_connectTimeout = 5 * time.Second
)

type (
	// Crypt AFAIRE.
	Crypto interface {
		DecryptString(text string) (string, error)
	}

	// Config AFAIRE.
	Config struct {
		Host     string
		Port     int
		Username string
		Password string
		Database string
		MaxConns int
		Timeout  time.Duration
	}
)

// Connect AFAIRE.
func (cfg *Config) Connect(crypto Crypto, logger logger.Logger) (*Client, error) {
	password := cfg.Password

	if crypto != nil {
		var err error

		password, err = crypto.DecryptString(password)
		if err != nil {
			return nil, err
		}
	}

	if cfg.MaxConns == 0 {
		cfg.MaxConns = _poolMaxConns
	}

	if cfg.Timeout == 0 {
		cfg.Timeout = _connectTimeout
	}

	uri := fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s?pool_max_conns=%d",
		cfg.Username,
		password,
		cfg.Host,
		cfg.Port,
		cfg.Database,
		cfg.MaxConns,
	)

	client := NewClient(logger)

	ctx, cancel := client.ContextWT(cfg.Timeout)
	defer cancel()

	return client, client.Connect(ctx, uri)
}

/*
######################################################################################################## @(°_°)@ #######
*/
