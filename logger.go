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

func (pgxl *pgxLogger) Log(_ context.Context, level pgx.LogLevel, msg string, ctx map[string]interface{}) {
	kv := []interface{}{}

	for k, v := range ctx {
		kv = append(kv, k, v)
	}

	switch level {
	case pgx.LogLevelTrace:
		pgxl.Trace(msg, kv...)
	case pgx.LogLevelDebug:
		pgxl.Debug(msg, kv...)
	case pgx.LogLevelInfo:
		pgxl.Info(msg, kv...)
	case pgx.LogLevelWarn:
		pgxl.Warning(msg, kv...)
	case pgx.LogLevelError:
		pgxl.Error(msg, kv...)
	}
}

/*
######################################################################################################## @(°_°)@ #######
*/
