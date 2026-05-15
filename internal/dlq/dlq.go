package dlq

import (
	"context"
	"log/slog"
	"time"

	"github.com/EchoMessenger/ingestor/internal/config"
	kafkacfg "github.com/EchoMessenger/ingestor/internal/kafka"
	"github.com/IBM/sarama"
)

const sendTimeout = 5 * time.Second

// Producer отправляет необработанные сообщения в DLQ топик.
type Producer struct {
	prod  sarama.SyncProducer
	topic string
	log   *slog.Logger
}

func NewProducer(cfg *config.Config, log *slog.Logger) (*Producer, error) {
	// Используем общий config builder — TLS и SASL применяются автоматически.
	// Без этого при SASL-защищённом брокере получаем EOF на этапе handshake.
	sc := kafkacfg.NewSaramaConfig(cfg)
	sc.Producer.Return.Successes = true
	sc.Producer.RequiredAcks = sarama.WaitForAll
	sc.Producer.Retry.Max = 3

	prod, err := sarama.NewSyncProducer(cfg.KafkaBrokers, sc)
	if err != nil {
		return nil, err
	}

	return &Producer{prod: prod, topic: cfg.DLQTopic, log: log}, nil
}

// Send отправляет сырые байты в DLQ с заголовками для диагностики.
func (p *Producer) Send(sourceTopic, key string, value []byte, reason string) {
	ctx, cancel := context.WithTimeout(context.Background(), sendTimeout)
	defer cancel()

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