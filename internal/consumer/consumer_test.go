package consumer

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/EchoMessenger/ingestor/internal/handler"
	"github.com/IBM/sarama"
)

type fakeRegistry struct {
	h  handler.Handler
	ok bool
}

func (r fakeRegistry) Get(topic string) (handler.Handler, bool) {
	return r.h, r.ok
}

type fakeHandler struct {
	err   error
	count int
}

func (h *fakeHandler) Topic() string { return "tinode.message-events" }
func (h *fakeHandler) Stop()         {}
func (h *fakeHandler) Handle(msg *sarama.ConsumerMessage) error {
	h.count++
	return h.err
}

type fakeSession struct {
	marked []*sarama.ConsumerMessage
}

func (s *fakeSession) Claims() map[string][]int32 {
	return map[string][]int32{"tinode.message-events": {0}}
}
func (s *fakeSession) MemberID() string    { return "member" }
func (s *fakeSession) GenerationID() int32 { return 1 }
func (s *fakeSession) MarkOffset(topic string, partition int32, offset int64, metadata string) {
}
func (s *fakeSession) Commit() {}
func (s *fakeSession) ResetOffset(topic string, partition int32, offset int64, metadata string) {
}
func (s *fakeSession) MarkMessage(msg *sarama.ConsumerMessage, metadata string) {
	s.marked = append(s.marked, msg)
}
func (s *fakeSession) Context() context.Context { return context.Background() }

type fakeClaim struct {
	topic     string
	partition int32
	messages  chan *sarama.ConsumerMessage
}

func newClaim(topic string, messages ...*sarama.ConsumerMessage) *fakeClaim {
	ch := make(chan *sarama.ConsumerMessage, len(messages))
	for _, msg := range messages {
		ch <- msg
	}
	close(ch)
	return &fakeClaim{topic: topic, messages: ch}
}

func (c *fakeClaim) Topic() string                            { return c.topic }
func (c *fakeClaim) Partition() int32                         { return c.partition }
func (c *fakeClaim) InitialOffset() int64                     { return 0 }
func (c *fakeClaim) HighWaterMarkOffset() int64               { return 0 }
func (c *fakeClaim) Messages() <-chan *sarama.ConsumerMessage { return c.messages }

func consumerLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

type fakeConsumerGroup struct {
	consumeCalls int
	closed       bool
}

func (g *fakeConsumerGroup) Consume(ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error {
	g.consumeCalls++
	return nil
}
func (g *fakeConsumerGroup) Errors() <-chan error { return nil }
func (g *fakeConsumerGroup) Close() error {
	g.closed = true
	return nil
}
func (g *fakeConsumerGroup) Pause(partitions map[string][]int32)  {}
func (g *fakeConsumerGroup) Resume(partitions map[string][]int32) {}
func (g *fakeConsumerGroup) PauseAll()                            {}
func (g *fakeConsumerGroup) ResumeAll()                           {}

func TestSetupCallsOnReadyOnce(t *testing.T) {
	g := newGroup(fakeRegistry{}, nil, consumerLogger())
	var ready int
	g.OnReady(func() { ready++ })
	sess := &fakeSession{}

	if err := g.Setup(sess); err != nil {
		t.Fatalf("Setup returned error: %v", err)
	}
	if err := g.Setup(sess); err != nil {
		t.Fatalf("second Setup returned error: %v", err)
	}
	if ready != 1 {
		t.Fatalf("ready calls = %d, want 1", ready)
	}
	if err := g.Cleanup(sess); err != nil {
		t.Fatalf("Cleanup returned error: %v", err)
	}
}

func TestConsumeClaimMarksUnknownTopicMessages(t *testing.T) {
	g := newGroup(fakeRegistry{ok: false}, nil, consumerLogger())
	sess := &fakeSession{}
	claim := newClaim("tinode.unknown", &sarama.ConsumerMessage{Offset: 1}, &sarama.ConsumerMessage{Offset: 2})

	if err := g.ConsumeClaim(sess, claim); err != nil {
		t.Fatalf("ConsumeClaim returned error: %v", err)
	}
	if len(sess.marked) != 2 {
		t.Fatalf("marked = %d, want 2", len(sess.marked))
	}
}

func TestConsumeClaimHandlesSuccessAndError(t *testing.T) {
	tests := []struct {
		name       string
		handlerErr error
		wantDLQ    int
		wantErrors int64
		wantOK     int64
	}{
		{name: "success", wantOK: 1},
		{name: "handler error", handlerErr: errors.New("bad protobuf"), wantDLQ: 1, wantErrors: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &fakeHandler{err: tt.handlerErr}
			var dlqCalls int
			var dlqTopic, dlqKey, dlqReason string
			g := newGroup(fakeRegistry{h: h, ok: true}, func(topic, key string, value []byte, reason string) {
				dlqCalls++
				dlqTopic, dlqKey, dlqReason = topic, key, reason
			}, consumerLogger())
			sess := &fakeSession{}
			message := &sarama.ConsumerMessage{Topic: "tinode.message-events", Partition: 0, Offset: 7, Key: []byte("key"), Value: []byte("value"), Timestamp: time.Now()}

			if err := g.ConsumeClaim(sess, newClaim("tinode.message-events", message)); err != nil {
				t.Fatalf("ConsumeClaim returned error: %v", err)
			}
			if h.count != 1 || len(sess.marked) != 1 {
				t.Fatalf("handler count/marked = %d/%d, want 1/1", h.count, len(sess.marked))
			}
			if dlqCalls != tt.wantDLQ || g.errors != tt.wantErrors || g.processed != tt.wantOK {
				t.Fatalf("dlq/errors/processed = %d/%d/%d", dlqCalls, g.errors, g.processed)
			}
			if tt.wantDLQ == 1 && (dlqTopic != "tinode.message-events" || dlqKey != "key" || dlqReason != "bad protobuf") {
				t.Fatalf("dlq args = %s/%s/%s", dlqTopic, dlqKey, dlqReason)
			}
		})
	}
}

func TestRunnerRunClosesClientWhenContextCanceled(t *testing.T) {
	client := &fakeConsumerGroup{}
	r := &Runner{client: client, topics: []string{"tinode.message-events"}, log: consumerLogger()}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := r.Run(ctx); err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if client.consumeCalls != 1 {
		t.Fatalf("consume calls = %d, want 1", client.consumeCalls)
	}
	if !client.closed {
		t.Fatalf("client was not closed")
	}
}

func TestCleanupStopsHandlers(t *testing.T) {
	h := &fakeHandler{}
	g := newGroup(fakeRegistry{h: h, ok: true}, nil, consumerLogger())
	sess := &fakeSession{}

	if err := g.Setup(sess); err != nil {
		t.Fatalf("Setup failed: %v", err)
	}

	if err := g.Cleanup(sess); err != nil {
		t.Fatalf("Cleanup failed: %v", err)
	}
}

func TestOnReadyCallback(t *testing.T) {
	g := newGroup(fakeRegistry{}, nil, consumerLogger())
	var callCount int
	g.OnReady(func() { callCount++ })
	sess := &fakeSession{}

	g.Setup(sess)
	if callCount != 1 {
		t.Fatalf("OnReady not called or called %d times", callCount)
	}

	g.Setup(sess)
	if callCount != 1 {
		t.Fatalf("OnReady called multiple times: %d", callCount)
	}
}
