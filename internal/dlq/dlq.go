package dlq

import (
	"context"
	"log/slog"
	"time"

	"github.com/IBM/sarama"
)

const sendTimeout = 5 * time.Second

// Producer отправляет необработанные сообщения в DLQ топик.
type Producer struct {
	prod  sarama.SyncProducer
	topic string
	log   *slog.Logger
}

func NewProducer(brokers []string, topic string, log *slog.Logger) (*Producer, error) {
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Retry.Max = 3

	prod, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		return nil, err
	}

	return &Producer{prod: prod, topic: topic, log: log}, nil
}

// Send отправляет сырые байты в DLQ.
// key — оригинальный partition key, value — сырой protobuf.
func (p *Producer) Send(sourceTopic, key string, value []byte, reason string) {
	ctx, cancel := context.WithTimeout(context.Background(), sendTimeout)
	defer cancel()

	// Добавляем заголовки для диагностики
	headers := []sarama.RecordHeader{
		{Key: []byte("source_topic"), Value: []byte(sourceTopic)},
		{Key: []byte("reason"), Value: []byte(reason)},
	}

	msg := &sarama.ProducerMessage{
		Topic:   p.topic,
		Key:     sarama.StringEncoder(key),
		Value:   sarama.ByteEncoder(value),
		Headers: headers,
	}

	done := make(chan error, 1)
	go func() {
		_, _, err := p.prod.SendMessage(msg)
		done <- err
	}()

	select {
	case err := <-done:
		if err != nil {
			p.log.Error("dlq.send.failed",
				"source_topic", sourceTopic,
				"key", key,
				"reason", reason,
				"err", err,
			)
		} else {
			p.log.Warn("dlq.send.ok",
				"source_topic", sourceTopic,
				"key", key,
				"reason", reason,
				"payload_bytes", len(value),
			)
		}
	case <-ctx.Done():
		p.log.Error("dlq.send.timeout",
			"source_topic", sourceTopic,
			"key", key,
		)
	}
}

func (p *Producer) Close() error {
	return p.prod.Close()
}