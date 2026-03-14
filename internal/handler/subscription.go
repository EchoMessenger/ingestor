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

type subscriptionRow struct {
	LogID        string
	LogTimestamp time.Time
	Action       string
	Topic        string
	UserID       string
	DelID        *int32
	ReadID       *int32
	RecvID       *int32
	ModeWant     *string
	ModeGiven    *string
	Private      *string
}

type subscriptionHandler struct {
	topicName string
	batch     *chclient.Batch
	log       *slog.Logger
}

func newSubscriptionHandler(prefix string, ch *chclient.Client, mkBatch makeBatchFn, log *slog.Logger) *subscriptionHandler {
	h := &subscriptionHandler{
		topicName: topic(prefix, "subscription-events"),
		log:       log,
	}
	h.batch = mkBatch("subscription_log", func(ctx context.Context, rows []chclient.Row) error {
		return flushSubscriptionRows(ctx, ch, rows)
	})
	return h
}

func (h *subscriptionHandler) Topic() string { return h.topicName }
func (h *subscriptionHandler) Stop()         { h.batch.Stop() }

func (h *subscriptionHandler) Handle(msg *sarama.ConsumerMessage) error {
	var ev pbx.SubscriptionEvent
	if err := proto.Unmarshal(msg.Value, &ev); err != nil {
		return fmt.Errorf("subscription: unmarshal: %w", err)
	}

	row := subscriptionRow{
		LogID:        newUUID(),
		LogTimestamp: time.UnixMilli(msg.Timestamp.UnixMilli()).UTC(),
		Action:       ev.GetAction().String(),
		Topic:        ev.GetTopic(),
		UserID:       ev.GetUserId(),
		DelID:        nullableInt32(ev.GetDelId()),
		ReadID:       nullableInt32(ev.GetReadId()),
		RecvID:       nullableInt32(ev.GetRecvId()),
		Private:      nullableString(string(ev.GetPrivate())),
	}
	if mode := ev.GetMode(); mode != nil {
		row.ModeWant = nullableString(mode.GetWant())
		row.ModeGiven = nullableString(mode.GetGiven())
	}

	h.batch.Add(row)

	h.log.Debug("handler.subscription.add",
		"action", row.Action,
		"topic", row.Topic,
		"user_id", row.UserID,
	)
	return nil
}

func flushSubscriptionRows(ctx context.Context, ch *chclient.Client, rows []chclient.Row) error {
	batch, err := ch.Conn().PrepareBatch(ctx, `
		INSERT INTO subscription_log (
			log_id, log_timestamp,
			action, topic, user_id,
			del_id, read_id, recv_id,
			mode_want, mode_given,
			private
		)`)
	if err != nil {
		return fmt.Errorf("subscription_log: prepare: %w", err)
	}

	for _, r := range rows {
		row := r.(subscriptionRow)
		if err := batch.Append(
			row.LogID, row.LogTimestamp,
			row.Action, row.Topic, row.UserID,
			row.DelID, row.ReadID, row.RecvID,
			row.ModeWant, row.ModeGiven,
			row.Private,
		); err != nil {
			return fmt.Errorf("subscription_log: append: %w", err)
		}
	}

	return batch.Send()
}