package handler

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	chclient "github.com/EchoMessenger/ingestor/internal/clickhouse"
	pbx "github.com/EchoMessenger/ingestor/pbx"

	"github.com/IBM/sarama"
	"google.golang.org/protobuf/proto"
)

// search_log — таблицу нужно создать:
//
// CREATE TABLE search_log (
//   log_id UUID DEFAULT generateUUIDv4(),
//   log_timestamp DateTime64(3) DEFAULT now64(3),
//   user_id String,
//   query String
// ) ENGINE = MergeTree()
// PARTITION BY toYYYYMM(log_timestamp)
// ORDER BY (log_timestamp, user_id)
// TTL log_timestamp + INTERVAL 90 DAY;

type searchRow struct {
	LogID        string
	LogTimestamp time.Time
	UserID       string
	Query        string
}

type searchHandler struct {
	topicName string
	batch     *chclient.Batch
	log       *slog.Logger
}

func newSearchHandler(prefix string, ch *chclient.Client, mkBatch makeBatchFn, log *slog.Logger) *searchHandler {
	h := &searchHandler{
		topicName: topic(prefix, "search-queries"),
		log:       log,
	}
	h.batch = mkBatch("search_log", func(ctx context.Context, rows []chclient.Row) error {
		return flushSearchRows(ctx, ch, rows)
	})
	return h
}

func (h *searchHandler) Topic() string { return h.topicName }
func (h *searchHandler) Stop()         { h.batch.Stop() }

func (h *searchHandler) Handle(msg *sarama.ConsumerMessage) error {
	var ev pbx.SearchQuery
	if err := proto.Unmarshal(msg.Value, &ev); err != nil {
		return fmt.Errorf("search: unmarshal: %w", err)
	}

	row := searchRow{
		LogID:        newUUID(),
		LogTimestamp: time.UnixMilli(msg.Timestamp.UnixMilli()).UTC(),
		UserID:       ev.GetUserId(),
		Query:        ev.GetQuery(),
	}

	h.batch.Add(row)

	h.log.Debug("handler.search.add",
		"user_id", row.UserID,
		"query_len", len(row.Query),
	)
	return nil
}

func flushSearchRows(ctx context.Context, ch *chclient.Client, rows []chclient.Row) error {
	batch, err := ch.Conn().PrepareBatch(ctx, `
		INSERT INTO search_log (log_id, log_timestamp, user_id, query)`)
	if err != nil {
		return fmt.Errorf("search_log: prepare: %w", err)
	}

	for _, r := range rows {
		row := r.(searchRow)
		if err := batch.Append(
			row.LogID, row.LogTimestamp, row.UserID, row.Query,
		); err != nil {
			return fmt.Errorf("search_log: append: %w", err)
		}
	}

	return batch.Send()
}