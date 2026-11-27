package mysql

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
)

// ErrUnsupportedSQLMode indicates a connection without strict SQL mode.
var ErrUnsupportedSQLMode = errors.New("MySQL strict SQL mode is required")

type strictConnector struct{ driver.Connector }

func (c strictConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	wrapped := &strictConn{Conn: conn}
	if err := wrapped.checkMode(ctx); err != nil {
		return nil, errors.Join(err, conn.Close())
	}
	return wrapped, nil
}

type strictConn struct{ driver.Conn }

func (c *strictConn) checkMode(ctx context.Context) error {
	rows, err := c.Conn.(driver.QueryerContext).QueryContext(ctx, "SELECT @@SESSION.sql_mode", nil)
	if err != nil {
		return fmt.Errorf("read MySQL SQL mode: %w", err)
	}
	defer rows.Close()
	values := make([]driver.Value, 1)
	if err := rows.Next(values); err != nil {
		return fmt.Errorf("read MySQL SQL mode: %w", err)
	}
	var mode string
	switch value := values[0].(type) {
	case string:
		mode = value
	case []byte:
		mode = string(value)
	default:
		return ErrUnsupportedSQLMode
	}
	for _, entry := range strings.Split(mode, ",") {
		if entry == "STRICT_TRANS_TABLES" || entry == "STRICT_ALL_TABLES" {
			return nil
		}
	}
	return ErrUnsupportedSQLMode
}

func (c *strictConn) ResetSession(ctx context.Context) error {
	if err := c.Conn.(driver.SessionResetter).ResetSession(ctx); err != nil {
		return err
	}
	if err := c.checkMode(ctx); err != nil {
		return driver.ErrBadConn
	}
	return nil
}

func (c *strictConn) IsValid() bool {
	return c.Conn.(driver.Validator).IsValid()
}

func (c *strictConn) Ping(ctx context.Context) error {
	return c.Conn.(driver.Pinger).Ping(ctx)
}

func (c *strictConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	return c.Conn.(driver.ConnPrepareContext).PrepareContext(ctx, query)
}

func (c *strictConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return c.Conn.(driver.ConnBeginTx).BeginTx(ctx, opts)
}

func (c *strictConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	return c.Conn.(driver.QueryerContext).QueryContext(ctx, query, args)
}

func (c *strictConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, query, args)
}

func (c *strictConn) CheckNamedValue(value *driver.NamedValue) error {
	return c.Conn.(driver.NamedValueChecker).CheckNamedValue(value)
}
