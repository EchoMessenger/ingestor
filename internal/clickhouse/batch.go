package clickhouse

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// Row — одна строка для вставки, уже преобразованная из protobuf.
// Конкретный тип определяется каждым handler'ом.
type Row interface{}

// FlushFunc вызывается когда буфер готов к сбросу.
// Получает срез строк и должна вставить их в ClickHouse.
type FlushFunc func(ctx context.Context, rows []Row) error

// Batch буферизует строки и сбрасывает их по размеру или таймеру.
type Batch struct {
	name      string
	maxSize   int
	timeout   time.Duration
	flushFn   FlushFunc
	dlqFn     func(rows []Row, reason string)
	retryMax  int
	baseDelay time.Duration
	log       *slog.Logger

	mu   sync.Mutex
	rows []Row

	flushCh chan struct{}
	stopCh  chan struct{}
	wg      sync.WaitGroup
}

func NewBatch(
	name string,
	maxSize int,
	timeout time.Duration,
	retryMax int,
	baseDelay time.Duration,
	flushFn FlushFunc,
	dlqFn func(rows []Row, reason string),
	log *slog.Logger,
) *Batch {
	b := &Batch{
		name:      name,
		maxSize:   maxSize,
		timeout:   timeout,
		flushFn:   flushFn,
		dlqFn:     dlqFn,
		retryMax:  retryMax,
		baseDelay: baseDelay,
		log:       log,
		rows:      make([]Row, 0, maxSize),
		flushCh:   make(chan struct{}, 1),
		stopCh:    make(chan struct{}),
	}

	b.wg.Add(1)
	go b.loop()

	return b
}

// Add добавляет строку в буфер. Если буфер достиг maxSize — инициирует flush.
func (b *Batch) Add(row Row) {
	b.mu.Lock()
	b.rows = append(b.rows, row)
	full := len(b.rows) >= b.maxSize
	b.mu.Unlock()

	if full {
		b.triggerFlush()
	}
}

func (b *Batch) triggerFlush() {
	select {
	case b.flushCh <- struct{}{}:
	default:
		// flush уже запланирован
	}
}

func (b *Batch) loop() {
	defer b.wg.Done()
	ticker := time.NewTicker(b.timeout)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			b.flush("timer")

		case <-b.flushCh:
			b.flush("size")
			// Сбрасываем таймер чтобы не делать двойной flush
			ticker.Reset(b.timeout)

		case <-b.stopCh:
			// Финальный flush перед остановкой
			b.flush("shutdown")
			return
		}
	}
}

func (b *Batch) flush(reason string) {
	b.mu.Lock()
	if len(b.rows) == 0 {
		b.mu.Unlock()
		return
	}
	rows := b.rows
	b.rows = make([]Row, 0, b.maxSize)
	b.mu.Unlock()

	b.log.Info("batch.flush",
		"table", b.name,
		"reason", reason,
		"rows", len(rows),
	)

	ctx := context.Background()
	if err := b.sendWithRetry(ctx, rows); err != nil {
		b.log.Error("batch.flush.failed",
			"table", b.name,
			"rows", len(rows),
			"err", err,
		)
		if b.dlqFn != nil {
			b.dlqFn(rows, err.Error())
		}
	}
}

func (b *Batch) sendWithRetry(ctx context.Context, rows []Row) error {
	var lastErr error
	delay := b.baseDelay

	for attempt := 0; attempt <= b.retryMax; attempt++ {
		if attempt > 0 {
			b.log.Warn("batch.retry",
				"table", b.name,
				"attempt", attempt,
				"delay_ms", delay.Milliseconds(),
				"err", lastErr,
			)
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return ctx.Err()
			}
			delay *= 2 // exponential backoff
		}

		if err := b.flushFn(ctx, rows); err != nil {
			lastErr = err
			continue
		}

		b.log.Debug("batch.flush.ok",
			"table", b.name,
			"rows", len(rows),
			"attempt", attempt,
		)
		return nil
	}

	return lastErr
}

// Stop останавливает фоновый loop и дожидается финального flush.
func (b *Batch) Stop() {
	close(b.stopCh)
	b.wg.Wait()
}