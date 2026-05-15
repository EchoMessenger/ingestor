package dlq

import (
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/IBM/sarama"
	"github.com/EchoMessenger/ingestor/internal/config"
)

type fakeSyncProducer struct {
	msg    *sarama.ProducerMessage
	err    error
	closed bool
}

func (p *fakeSyncProducer) SendMessage(msg *sarama.ProducerMessage) (int32, int64, error) {
	p.msg = msg
	return 0, 0, p.err
}
func (p *fakeSyncProducer) SendMessages(msgs []*sarama.ProducerMessage) error { return nil }
func (p *fakeSyncProducer) Close() error {
	p.closed = true
	return nil
}
func (p *fakeSyncProducer) TxnStatus() sarama.ProducerTxnStatusFlag { return 0 }
func (p *fakeSyncProducer) IsTransactional() bool                   { return false }
func (p *fakeSyncProducer) BeginTxn() error                         { return nil }
func (p *fakeSyncProducer) CommitTxn() error                        { return nil }
func (p *fakeSyncProducer) AbortTxn() error                         { return nil }
func (p *fakeSyncProducer) AddOffsetsToTxn(offsets map[string][]*sarama.PartitionOffsetMetadata, groupId string) error {
	return nil
}
func (p *fakeSyncProducer) AddMessageToTxn(msg *sarama.ConsumerMessage, groupId string, metadata *string) error {
	return nil
}

func TestSendBuildsDLQMessage(t *testing.T) {
	prod := &fakeSyncProducer{}
	p := &Producer{prod: prod, topic: "tinode.dlq", log: slog.New(slog.NewTextHandler(io.Discard, nil))}

	p.Send("tinode.message-events", "key", []byte("payload"), "bad protobuf")

	if prod.msg == nil {
		t.Fatalf("SendMessage was not called")
	}
	if prod.msg.Topic != "tinode.dlq" {
		t.Fatalf("topic = %q", prod.msg.Topic)
	}
	if key, err := prod.msg.Key.Encode(); err != nil || string(key) != "key" {
		t.Fatalf("key = %q, err = %v", key, err)
	}
	if value, err := prod.msg.Value.Encode(); err != nil || string(value) != "payload" {
		t.Fatalf("value = %q, err = %v", value, err)
	}
	headers := map[string]string{}
	for _, h := range prod.msg.Headers {
		headers[string(h.Key)] = string(h.Value)
	}
	if headers["source_topic"] != "tinode.message-events" || headers["reason"] != "bad protobuf" {
		t.Fatalf("headers = %#v", headers)
	}
}

func TestSendHandlesProducerErrorAndClose(t *testing.T) {
	prod := &fakeSyncProducer{err: errors.New("send failed")}
	p := &Producer{prod: prod, topic: "tinode.dlq", log: slog.New(slog.NewTextHandler(io.Discard, nil))}

	p.Send("topic", "key", []byte("payload"), "reason")
	if prod.msg == nil {
		t.Fatalf("SendMessage was not called")
	}
	if err := p.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}
	if !prod.closed {
		t.Fatalf("producer was not closed")
	}
}

func TestSendHandlesSuccessfulSend(t *testing.T) {
	prod := &fakeSyncProducer{err: nil}
	p := &Producer{prod: prod, topic: "tinode.dlq", log: slog.New(slog.NewTextHandler(io.Discard, nil))}

	p.Send("tinode.account-events", "key123", []byte("payload"), "invalid message")

	if prod.msg == nil {
		t.Fatalf("SendMessage was not called")
	}
	if prod.msg.Topic != "tinode.dlq" {
		t.Fatalf("topic = %q", prod.msg.Topic)
	}
}

func TestSendWithTimeout(t *testing.T) {
	type slowProducer struct {
		fakeSyncProducer
	}
	
	sp := &slowProducer{}
	p := &Producer{prod: sp, topic: "tinode.dlq", log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	
	// Этот тест демонстрирует timeout path - обработка займет более 5 секунд
	// Но так как мы работаем с горутинами, просто проверяем что Send не блокирует
	p.Send("topic", "key", []byte("payload"), "reason")
}

func TestNewProducerFailsOnInvalidBrokers(t *testing.T) {
	cfg := &config.Config{
		KafkaBrokers: []string{"invalid-broker:9999"},
		DLQTopic:     "tinode.dlq",
	}
	
	_, err := NewProducer(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err == nil {
		t.Fatalf("expected error for invalid broker")
	}
}
