/*
------------------------------------------------------------------------------------------------------------------------
####### pgsql ####### (c) 2020-2021 mls-361 ######################################################## MIT License #######
------------------------------------------------------------------------------------------------------------------------
*/

package pgsql

import (
	"context"

	"github.com/jackc/pgx/v4"
	"github.com/mls-361/logger"
)

type (
	pgxLogger struct {
		logger.Logger
	}
)

func (pl *pgxLogger) Log(_ context.Context, level pgx.LogLevel, msg string, ctx map[string]interface{}) {
	kv := []interface{}{}

	for k, v := range ctx {
		kv = append(kv, k, v)
	}

	switch level {
	case pgx.LogLevelTrace:
		pl.Trace(msg, kv...)
	case pgx.LogLevelDebug:
		pl.Debug(msg, kv...)
	case pgx.LogLevelInfo:
		pl.Info(msg, kv...)
	case pgx.LogLevelWarn:
		pl.Warning(msg, kv...)
	case pgx.LogLevelError:
		pl.Error(msg, kv...)
	}
}

/*
######################################################################################################## @(°_°)@ #######
*/
