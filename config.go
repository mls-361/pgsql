/*
------------------------------------------------------------------------------------------------------------------------
####### pgsql ####### (c) 2020-2021 mls-361 ######################################################## MIT License #######
------------------------------------------------------------------------------------------------------------------------
*/

package pgsql

import (
	"time"

	"github.com/mls-361/logger"
)

type (
	// Crypto AFAIRE.
	Crypto interface {
		DecryptString(text string) (string, error)
	}

	// Config AFAIRE.
	Config struct {
		Host              string
		Port              int32
		Username          string
		Password          string
		Database          string
		MaxConns          int32
		MinConns          int32
		ConnLifeTime      time.Duration
		ConnIdleTime      time.Duration
		HealthCheckPeriod time.Duration
		ConnectTimeout    time.Duration
	}
)

// NewClient AFAIRE.
func (cfg *Config) NewClient(crypto Crypto, logger logger.Logger) (*Client, error) {
	if crypto != nil {
		p, err := crypto.DecryptString(cfg.Password)
		if err != nil {
			return nil, err
		}

		cfg.Password = p
	}

	return newClient(cfg, logger), nil
}

// Connect AFAIRE.
func (cfg *Config) Connect(crypto Crypto, logger logger.Logger) (*Client, error) {
	client, err := cfg.NewClient(crypto, logger)
	if err != nil {
		return nil, err
	}

	return client, client.Connect()
}

/*
######################################################################################################## @(°_°)@ #######
*/
