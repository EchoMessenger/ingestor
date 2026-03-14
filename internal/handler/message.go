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

type messageRow struct {
	LogID        string
	LogTimestamp time.Time
	Action       string
	MsgTopic     string
	MsgFromUser  *string
	MsgTimestamp int64
	MsgDeletedAt *int64
	MsgSeqID     int32
	MsgHead      map[string]string
	MsgContent   *string
}

type messageHandler struct {
	topicName string
	batch     *chclient.Batch
	log       *slog.Logger
}

func newMessageHandler(prefix string, ch *chclient.Client, mkBatch makeBatchFn, log *slog.Logger) *messageHandler {
	h := &messageHandler{
		topicName: topic(prefix, "message-events"),
		log:       log,
	}
	h.batch = mkBatch("message_log", func(ctx context.Context, rows []chclient.Row) error {
		return flushMessageRows(ctx, ch, rows)
	})
	return h
}

func (h *messageHandler) Topic() string { return h.topicName }
func (h *messageHandler) Stop()         { h.batch.Stop() }

func (h *messageHandler) Handle(msg *sarama.ConsumerMessage) error {
	var ev pbx.MessageEvent
	if err := proto.Unmarshal(msg.Value, &ev); err != nil {
		return fmt.Errorf("message: unmarshal: %w", err)
	}

	m := ev.GetMsg()
	row := messageRow{
		LogID:        newUUID(),
		LogTimestamp: time.UnixMilli(msg.Timestamp.UnixMilli()).UTC(),
		Action:       ev.GetAction().String(),
		MsgTopic:     m.GetTopic(),
		MsgFromUser:  nullableString(m.GetFromUserId()),
		MsgTimestamp: m.GetTimestamp(),
		MsgDeletedAt: nullableInt64(m.GetDeletedAt()),
		MsgSeqID:     m.GetSeqId(),
		MsgHead:      mapBytesToString(m.GetHead()),
		MsgContent:   nullableString(string(m.GetContent())),
	}

	h.batch.Add(row)

	h.log.Debug("handler.message.add",
		"action", row.Action,
		"topic", row.MsgTopic,
		"seq_id", row.MsgSeqID,
	)
	return nil
}

func flushMessageRows(ctx context.Context, ch *chclient.Client, rows []chclient.Row) error {
	batch, err := ch.Conn().PrepareBatch(ctx, `
		INSERT INTO message_log (
			log_id, log_timestamp,
			action,
			msg_topic, msg_from_user_id,
			msg_timestamp, msg_deleted_at, msg_seq_id,
			msg_head, msg_content
		)`)
	if err != nil {
		return fmt.Errorf("message_log: prepare: %w", err)
	}

	for _, r := range rows {
		row := r.(messageRow)
		if err := batch.Append(
			row.LogID, row.LogTimestamp,
			row.Action,
			row.MsgTopic, row.MsgFromUser,
			row.MsgTimestamp, row.MsgDeletedAt, row.MsgSeqID,
			row.MsgHead, row.MsgContent,
		); err != nil {
			return fmt.Errorf("message_log: append: %w", err)
		}
	}

	return batch.Send()
}