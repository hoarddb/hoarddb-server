package main

import (
	"context"
	"errors"
	"fmt"
	"net"

	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgconn"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/hoarddb/hoarddb-server/ringclient"
)

// errorKind buckets an error for the report's errors_by_type map.
func errorKind(err error) string {
	var refusal *serverError
	var netErr net.Error
	var mongoErr mongo.CommandError
	var mysqlErr *mysql.MySQLError
	var postgresErr *pgconn.PgError
	var ringRefusal *ringclient.Error
	switch {
	case errors.As(err, &refusal):
		return "server_" + refusal.code.String()
	case errors.As(err, &ringRefusal):
		return "server_ring_" + ringRefusal.Code.String()
	case errors.As(err, &mongoErr):
		return fmt.Sprintf("server_mongo_%d", mongoErr.Code)
	case errors.As(err, &mysqlErr):
		return fmt.Sprintf("server_mysql_%d", mysqlErr.Number)
	case errors.As(err, &postgresErr):
		return "server_postgres_" + postgresErr.Code
	case errors.Is(err, context.DeadlineExceeded), errors.As(err, &netErr) && netErr.Timeout():
		return "timeout"
	default:
		return "io_or_decode"
	}
}

// isDesynced reports whether the socket can no longer be trusted. After a
// timeout the reply may still arrive and would be read as the next request's.
// A refusal the server sent, on the legacy wire or the ring wire, leaves the
// connection in step.
func isDesynced(err error) bool {
	var refusal *serverError
	var ringRefusal *ringclient.Error
	return !errors.As(err, &refusal) && !errors.As(err, &ringRefusal)
}
