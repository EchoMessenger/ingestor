package handler

import (
	"fmt"
	"log/slog"
	"strings"
	"time"

	chclient "github.com/EchoMessenger/ingestor/internal/clickhouse"
	"github.com/EchoMessenger/ingestor/internal/config"
	"github.com/IBM/sarama"
)

// Handler обрабатывает сообщения из одного Kafka топика.
type Handler interface {
	// Topic возвращает полное имя топика (с префиксом).
	Topic() string
	// Handle десериализует сообщение и добавляет строку в batch.
	Handle(msg *sarama.ConsumerMessage) error
	// Stop останавливает batch (финальный flush).
	Stop()
}

// Registry хранит маппинг topic → Handler.
type Registry struct {
	handlers map[string]Handler
	log      *slog.Logger
}

func NewRegistry(cfg *config.Config, ch *chclient.Client, dlqFn func(topic, key string, value []byte, reason string), log *slog.Logger) (*Registry, error) {
	prefix := strings.TrimSuffix(cfg.KafkaTopicPrefix, ".")

	makeBatchOpts := func(name string, flushFn chclient.FlushFunc) *chclient.Batch {
		dlqRowFn := func(rows []chclient.Row, reason string) {
			// При ошибке batch'а логируем количество потерянных строк.
			// Сами сырые байты уже в DLQ на уровне consumer'а при decode-ошибке.
			log.Error("batch.dlq",
				"table", name,
				"lost_rows", len(rows),
				"reason", reason,
			)
		}
		return chclient.NewBatch(
			name,
			cfg.BatchSize,
			cfg.BatchTimeout,
			cfg.RetryMax,
			cfg.RetryBaseDelay,
			flushFn,
			dlqRowFn,
			log,
		)
	}

	r := &Registry{
		handlers: make(map[string]Handler),
		log:      log,
	}

	handlers := []Handler{
		newAccountHandler(prefix, ch, makeBatchOpts, log),
		newTopicHandler(prefix, ch, makeBatchOpts, log),
		newSubscriptionHandler(prefix, ch, makeBatchOpts, log),
		newMessageHandler(prefix, ch, makeBatchOpts, log),
		// searchHandler зарегистрирован и готов к работе, но топик tinode.search-queries
		// останется пустым пока в tinode.conf не выставлен "filters": { "find": true }.
		// Consumer корректно обработает пустой топик — ничего не сломается.
		// См. подробности: internal/handler/search.go
		newSearchHandler(prefix, ch, makeBatchOpts, log),
		newFirehoseHandshakeHandler(prefix, ch, makeBatchOpts, log),
		newFirehoseAuthHandler(prefix, ch, makeBatchOpts, log),
		newFirehoseAccountMgmtHandler(prefix, ch, makeBatchOpts, log),
		newFirehoseSubscriptionsHandler(prefix, ch, makeBatchOpts, log),
		newFirehoseMessagesHandler(prefix, ch, makeBatchOpts, log),
		newFirehoseQueriesHandler(prefix, ch, makeBatchOpts, log),
		newFirehoseUpdatesHandler(prefix, ch, makeBatchOpts, log),
		newFirehoseDeletionsHandler(prefix, ch, makeBatchOpts, log),
		newFirehoseNotificationsHandler(prefix, ch, makeBatchOpts, log),
	}

	for _, h := range handlers {
		r.handlers[h.Topic()] = h
	}

	return r, nil
}

func (r *Registry) Get(topic string) (Handler, bool) {
	h, ok := r.handlers[topic]
	return h, ok
}

func (r *Registry) StopAll() {
	for _, h := range r.handlers {
		h.Stop()
	}
}

// ---- helpers ----

type makeBatchFn func(name string, flushFn chclient.FlushFunc) *chclient.Batch

func nowMs() time.Time {
	return time.Now().UTC()
}

func topic(prefix, name string) string {
	return fmt.Sprintf("%s.%s", prefix, name)
}

// nullableString возвращает указатель на строку или nil если пустая.
func nullableString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func nullableInt32(v int32) *int32 {
	if v == 0 {
		return nil
	}
	return &v
}

func nullableInt64(v int64) *int64 {
	if v == 0 {
		return nil
	}
	return &v
}

func nullableBool(v bool) *bool {
	return &v
}

// mapToString конвертирует map[string][]byte в map[string]string.
func mapBytesToString(m map[string][]byte) map[string]string {
	if len(m) == 0 {
		return map[string]string{}
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = string(v)
	}
	return out
}