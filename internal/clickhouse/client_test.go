package clickhouse

import (
	"context"
	"errors"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// mockConn - mock для driver.Conn
type mockConn struct {
	pingErr  error
	closeErr error
	closed   bool
}

func (m *mockConn) Ping(ctx context.Context) error {
	return m.pingErr
}

func (m *mockConn) Close() error {
	m.closed = true
	return m.closeErr
}

func (m *mockConn) Exec(ctx context.Context, query string, args ...any) error {
	return nil
}

func (m *mockConn) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	return nil, nil
}

func (m *mockConn) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	return nil
}

func (m *mockConn) PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error) {
	return nil, nil
}

func (m *mockConn) Select(ctx context.Context, dest any, query string, args ...any) error {
	return nil
}

func (m *mockConn) ServerVersion() (*driver.ServerVersion, error) {
	return nil, nil
}

func (m *mockConn) Stats() driver.Stats {
	return driver.Stats{}
}

func (m *mockConn) Contributors() []string {
	return nil
}

func (m *mockConn) AsyncInsert(ctx context.Context, query string, wait bool, args ...any) error {
	return nil
}

func TestConnReturnsTheConnection(t *testing.T) {
	mock := &mockConn{}
	c := &Client{conn: mock}

	conn := c.Conn()
	if conn == nil {
		t.Fatalf("Conn() returned nil")
	}
}

func TestCloseClosesTheConnection(t *testing.T) {
	mock := &mockConn{}
	c := &Client{conn: mock}

	if err := c.Close(); err != nil {
		t.Fatalf("Close() returned error: %v", err)
	}
	if !mock.closed {
		t.Fatalf("Close() did not close the connection")
	}
}

func TestCloseReturnsError(t *testing.T) {
	closeErr := errors.New("close failed")
	mock := &mockConn{closeErr: closeErr}
	c := &Client{conn: mock}

	if err := c.Close(); !errors.Is(err, closeErr) {
		t.Fatalf("Close() error = %v, want %v", err, closeErr)
	}
}

func TestNewClientFailsWithInvalidAddress(t *testing.T) {
	// Invalid address will cause Open to succeed but Ping to fail
	c, err := NewClient("invalid-host:9999", "default", "default", "")
	if err == nil {
		t.Fatalf("NewClient should have failed with invalid host")
	}
	if c != nil {
		t.Fatalf("NewClient returned non-nil client on error")
	}
}
