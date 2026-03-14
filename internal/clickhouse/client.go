package clickhouse

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// Client обёртка над clickhouse-go/v2.
type Client struct {
	conn driver.Conn
}

func NewClient(addr, db, user, password string) (*Client, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Database: db,
			Username: user,
			Password: password,
		},
		Settings: clickhouse.Settings{
			"async_insert":          0, // вставляем батчами сами
			"wait_for_async_insert": 1,
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("clickhouse.Open: %w", err)
	}

	if err := conn.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("clickhouse.Ping: %w", err)
	}

	return &Client{conn: conn}, nil
}

func (c *Client) Conn() driver.Conn {
	return c.conn
}

func (c *Client) Close() error {
	return c.conn.Close()
}