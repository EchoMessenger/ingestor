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

// ---- account_log row ----

type accountRow struct {
	LogID        string
	LogTimestamp time.Time
	Action       string
	UserID       string
	DefaultAcsAuth *string
	DefaultAcsAnon *string
	Public       *string
	Tags         []string
}

// ---- handler ----

type accountHandler struct {
	topicName string
	batch     *chclient.Batch
	log       *slog.Logger
}

func newAccountHandler(prefix string, ch *chclient.Client, mkBatch makeBatchFn, log *slog.Logger) *accountHandler {
	h := &accountHandler{
		topicName: topic(prefix, "account-events"),
		log:       log,
	}

	h.batch = mkBatch("account_log", func(ctx context.Context, rows []chclient.Row) error {
		return flushAccountRows(ctx, ch, rows)
	})

	return h
}

func (h *accountHandler) Topic() string { return h.topicName }
func (h *accountHandler) Stop()         { h.batch.Stop() }

func (h *accountHandler) Handle(msg *sarama.ConsumerMessage) error {
	var ev pbx.AccountEvent
	if err := proto.Unmarshal(msg.Value, &ev); err != nil {
		return fmt.Errorf("account: unmarshal: %w", err)
	}

	row := accountRow{
		LogID:        newUUID(),
		LogTimestamp: time.UnixMilli(msg.Timestamp.UnixMilli()).UTC(),
		Action:       ev.GetAction().String(),
		UserID:       ev.GetUserId(),
		Tags:         ev.GetTags(),
		Public:       nullableString(string(ev.GetPublic())),
	}
	if acs := ev.GetDefaultAcs(); acs != nil {
		row.DefaultAcsAuth = nullableString(acs.GetAuth())
		row.DefaultAcsAnon = nullableString(acs.GetAnon())
	}

	h.batch.Add(row)

	h.log.Debug("handler.account.add",
		"action", row.Action,
		"user_id", row.UserID,
	)
	return nil
}

func flushAccountRows(ctx context.Context, ch *chclient.Client, rows []chclient.Row) error {
	batch, err := ch.Conn().PrepareBatch(ctx, `
		INSERT INTO account_log (
			log_id, log_timestamp,
			action, user_id,
			default_acs_auth, default_acs_anon,
			public, tags
		)`)
	if err != nil {
		return fmt.Errorf("account_log: prepare: %w", err)
	}

	for _, r := range rows {
		row := r.(accountRow)
		if err := batch.Append(
			row.LogID,
			row.LogTimestamp,
			row.Action,
			row.UserID,
			row.DefaultAcsAuth,
			row.DefaultAcsAnon,
			row.Public,
			row.Tags,
		); err != nil {
			return fmt.Errorf("account_log: append: %w", err)
		}
	}

	return batch.Send()
}