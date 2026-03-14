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

type topicRow struct {
	LogID        string
	LogTimestamp time.Time
	Action       string
	TopicName    string

	DescCreatedAt        *int64
	DescUpdatedAt        *int64
	DescTouchedAt        *int64
	DescDefacsAuth       *string
	DescDefacsAnon       *string
	DescAcsWant          *string
	DescAcsGiven         *string
	DescSeqID            *int32
	DescReadID           *int32
	DescRecvID           *int32
	DescDelID            *int32
	DescPublic           *string
	DescPrivate          *string
	DescTrusted          *string
	DescState            *string
	DescStateAt          *int64
	DescIsChan           *bool
	DescOnline           *bool
	DescLastSeenTime     *int64
	DescLastSeenUA       *string
}

type topicHandler struct {
	topicName string
	batch     *chclient.Batch
	log       *slog.Logger
}

func newTopicHandler(prefix string, ch *chclient.Client, mkBatch makeBatchFn, log *slog.Logger) *topicHandler {
	h := &topicHandler{
		topicName: topic(prefix, "topic-events"),
		log:       log,
	}
	h.batch = mkBatch("topic_log", func(ctx context.Context, rows []chclient.Row) error {
		return flushTopicRows(ctx, ch, rows)
	})
	return h
}

func (h *topicHandler) Topic() string { return h.topicName }
func (h *topicHandler) Stop()         { h.batch.Stop() }

func (h *topicHandler) Handle(msg *sarama.ConsumerMessage) error {
	var ev pbx.TopicEvent
	if err := proto.Unmarshal(msg.Value, &ev); err != nil {
		return fmt.Errorf("topic: unmarshal: %w", err)
	}

	row := topicRow{
		LogID:        newUUID(),
		LogTimestamp: time.UnixMilli(msg.Timestamp.UnixMilli()).UTC(),
		Action:       ev.GetAction().String(),
		TopicName:    ev.GetName(),
	}

	if d := ev.GetDesc(); d != nil {
		row.DescCreatedAt = nullableInt64(d.GetCreatedAt())
		row.DescUpdatedAt = nullableInt64(d.GetUpdatedAt())
		row.DescTouchedAt = nullableInt64(d.GetTouchedAt())
		row.DescSeqID = nullableInt32(d.GetSeqId())
		row.DescReadID = nullableInt32(d.GetReadId())
		row.DescRecvID = nullableInt32(d.GetRecvId())
		row.DescDelID = nullableInt32(d.GetDelId())
		row.DescPublic = nullableString(string(d.GetPublic()))
		row.DescPrivate = nullableString(string(d.GetPrivate()))
		row.DescTrusted = nullableString(string(d.GetTrusted()))
		row.DescState = nullableString(d.GetState())
		row.DescStateAt = nullableInt64(d.GetStateAt())
		row.DescLastSeenTime = nullableInt64(d.GetLastSeenTime())
		row.DescLastSeenUA = nullableString(d.GetLastSeenUserAgent())
		isChan := d.GetIsChan()
		row.DescIsChan = &isChan
		online := d.GetOnline()
		row.DescOnline = &online

		if da := d.GetDefacs(); da != nil {
			row.DescDefacsAuth = nullableString(da.GetAuth())
			row.DescDefacsAnon = nullableString(da.GetAnon())
		}
		if acs := d.GetAcs(); acs != nil {
			row.DescAcsWant = nullableString(acs.GetWant())
			row.DescAcsGiven = nullableString(acs.GetGiven())
		}
	}

	h.batch.Add(row)

	h.log.Debug("handler.topic.add",
		"action", row.Action,
		"topic", row.TopicName,
	)
	return nil
}

func flushTopicRows(ctx context.Context, ch *chclient.Client, rows []chclient.Row) error {
	batch, err := ch.Conn().PrepareBatch(ctx, `
		INSERT INTO topic_log (
			log_id, log_timestamp, action, topic_name,
			desc_created_at, desc_updated_at, desc_touched_at,
			desc_defacs_auth, desc_defacs_anon,
			desc_acs_want, desc_acs_given,
			desc_seq_id, desc_read_id, desc_recv_id, desc_del_id,
			desc_public, desc_private, desc_trusted,
			desc_state, desc_state_at,
			desc_is_chan, desc_online,
			desc_last_seen_time, desc_last_seen_user_agent
		)`)
	if err != nil {
		return fmt.Errorf("topic_log: prepare: %w", err)
	}

	for _, r := range rows {
		row := r.(topicRow)
		if err := batch.Append(
			row.LogID, row.LogTimestamp, row.Action, row.TopicName,
			row.DescCreatedAt, row.DescUpdatedAt, row.DescTouchedAt,
			row.DescDefacsAuth, row.DescDefacsAnon,
			row.DescAcsWant, row.DescAcsGiven,
			row.DescSeqID, row.DescReadID, row.DescRecvID, row.DescDelID,
			row.DescPublic, row.DescPrivate, row.DescTrusted,
			row.DescState, row.DescStateAt,
			row.DescIsChan, row.DescOnline,
			row.DescLastSeenTime, row.DescLastSeenUA,
		); err != nil {
			return fmt.Errorf("topic_log: append: %w", err)
		}
	}

	return batch.Send()
}