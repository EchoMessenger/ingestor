package clickhouse

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestBatchFlushesOnSizeAndShutdown(t *testing.T) {
	var mu sync.Mutex
	var batches [][]Row
	firstFlush := make(chan struct{}, 1)
	b := NewBatch("test", 2, time.Hour, 0, time.Millisecond, func(ctx context.Context, rows []Row) error {
		mu.Lock()
		defer mu.Unlock()
		batches = append(batches, append([]Row(nil), rows...))
		select {
		case firstFlush <- struct{}{}:
		default:
		}
		return nil
	}, nil, testLogger())

	b.Add("one")
	b.Add("two")
	select {
	case <-firstFlush:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for size flush")
	}
	b.Add("three")
	b.Stop()

	mu.Lock()
	defer mu.Unlock()
	if len(batches) != 2 {
		t.Fatalf("flush count = %d, want 2", len(batches))
	}
	if len(batches[0]) != 2 || len(batches[1]) != 1 {
		t.Fatalf("batch sizes = %d/%d, want 2/1", len(batches[0]), len(batches[1]))
	}
}

func TestBatchRetriesAndSendsRowsToDLQAfterFailure(t *testing.T) {
	var attempts int
	var dlqRows []Row
	var dlqReason string
	b := NewBatch("test", 1, time.Hour, 2, time.Millisecond, func(ctx context.Context, rows []Row) error {
		attempts++
		return errors.New("insert failed")
	}, func(rows []Row, reason string) {
		dlqRows = append([]Row(nil), rows...)
		dlqReason = reason
	}, testLogger())

	b.Add("row")
	b.Stop()

	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3", attempts)
	}
	if len(dlqRows) != 1 || dlqRows[0] != "row" {
		t.Fatalf("dlq rows = %#v, want row", dlqRows)
	}
	if dlqReason != "insert failed" {
		t.Fatalf("dlq reason = %q, want insert failed", dlqReason)
	}
}

func TestSendWithRetryStopsOnContextCancel(t *testing.T) {
	b := NewBatch("test", 10, time.Hour, 3, time.Hour, func(ctx context.Context, rows []Row) error {
		return errors.New("temporary")
	}, nil, testLogger())
	defer b.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := b.sendWithRetry(ctx, []Row{"row"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("sendWithRetry error = %v, want context.Canceled", err)
	}
}
