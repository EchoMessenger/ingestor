package consumer

import (
	"context"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/tls"
	"log/slog"
	"strings"
	"time"

	"sync"

	"github.com/EchoMessenger/ingestor/internal/config"
	"github.com/EchoMessenger/ingestor/internal/handler"
	"github.com/IBM/sarama"
	"github.com/xdg-go/scram"
)

// ---- SCRAM (копия из router) ----

var (
	SHA256 scram.HashGeneratorFcn = sha256.New
	SHA512 scram.HashGeneratorFcn = sha512.New
)

type xdgSCRAMClient struct {
	*scram.Client
	*scram.ClientConversation
	scram.HashGeneratorFcn
}

func (x *xdgSCRAMClient) Begin(u, p, a string) (err error) {
	x.Client, err = x.HashGeneratorFcn.NewClient(u, p, a)
	if err != nil {
		return err
	}
	x.ClientConversation = x.Client.NewConversation()
	return nil
}
func (x *xdgSCRAMClient) Step(c string) (string, error) { return x.ClientConversation.Step(c) }
func (x *xdgSCRAMClient) Done() bool                    { return x.ClientConversation.Done() }

// ---- Consumer group handler ----

// Group реализует sarama.ConsumerGroupHandler.
type Group struct {
	registry *handler.Registry
	dlqFn    func(topic, key string, value []byte, reason string)
	log      *slog.Logger

	// onReady вызывается однократно после первого успешного Setup.
	// Используется для readiness probe в k8s (/readyz endpoint).
	onReady     func()
	onReadyOnce sync.Once

	// Счётчики
	processed int64
	errors    int64
	dlqSent   int64
}

func NewGroup(registry *handler.Registry, dlqFn func(topic, key string, value []byte, reason string), log *slog.Logger) *Group {
	return &Group{
		registry: registry,
		dlqFn:    dlqFn,
		log:      log,
	}
}

// OnReady регистрирует callback который будет вызван однократно
// после того как consumer group успешно получила partition assignment.
func (g *Group) OnReady(fn func()) {
	g.onReady = fn
}

func (g *Group) Setup(sess sarama.ConsumerGroupSession) error {
	g.log.Info("consumer.group.setup",
		"member_id", sess.MemberID(),
		"generation", sess.GenerationID(),
		"claims", sess.Claims(),
	)
	// Сигнализируем о готовности однократно — при первом успешном rebalance.
	if g.onReady != nil {
		g.onReadyOnce.Do(g.onReady)
	}
	return nil
}

func (g *Group) Cleanup(sess sarama.ConsumerGroupSession) error {
	g.log.Info("consumer.group.cleanup",
		"member_id", sess.MemberID(),
		"processed", g.processed,
		"errors", g.errors,
		"dlq_sent", g.dlqSent,
	)
	return nil
}

func (g *Group) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	topic := claim.Topic()
	partition := claim.Partition()

	g.log.Info("consumer.claim.start",
		"topic", topic,
		"partition", partition,
	)

	h, ok := g.registry.Get(topic)
	if !ok {
		g.log.Warn("consumer.claim.no_handler",
			"topic", topic,
			"partition", partition,
		)
		// Читаем и коммитим без обработки чтобы не застрять
		for msg := range claim.Messages() {
			sess.MarkMessage(msg, "")
		}
		return nil
	}

	for msg := range claim.Messages() {
		if err := h.Handle(msg); err != nil {
			g.errors++
			g.log.Error("consumer.handle.error",
				"topic", topic,
				"partition", partition,
				"offset", msg.Offset,
				"err", err,
			)
			// Отправляем в DLQ сырые байты — handler сам решит retry
			key := string(msg.Key)
			g.dlqFn(topic, key, msg.Value, err.Error())
			g.dlqSent++
		} else {
			g.processed++
			g.log.Debug("consumer.handle.ok",
				"topic", topic,
				"partition", partition,
				"offset", msg.Offset,
			)
		}

		// Коммитим в любом случае — DLQ гарантирует сохранность
		sess.MarkMessage(msg, "")
	}

	return nil
}

// ---- Runner ----

// Runner запускает consumer group и перезапускает её при ребалансировке.
type Runner struct {
	client sarama.ConsumerGroup
	group  *Group
	topics []string
	log    *slog.Logger
}

func NewRunner(cfg *config.Config, group *Group, log *slog.Logger) (*Runner, error) {
	sc := sarama.NewConfig()
	sc.Version = sarama.V2_6_0_0
	sc.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
		sarama.NewBalanceStrategyRoundRobin(),
	}
	sc.Consumer.Offsets.Initial = sarama.OffsetOldest
	sc.Consumer.Offsets.AutoCommit.Enable = true
	sc.Consumer.Offsets.AutoCommit.Interval = 1 * time.Second

	if cfg.KafkaTLSEnable {
		sc.Net.TLS.Enable = true
		sc.Net.TLS.Config = &tls.Config{
			InsecureSkipVerify: false,
		}
	}

	if cfg.KafkaSASLEnable {
		sc.Net.SASL.Enable = true
		sc.Net.SASL.User = cfg.KafkaSASLUsername
		sc.Net.SASL.Password = cfg.KafkaSASLPassword

		switch strings.ToUpper(cfg.KafkaSASLMechanism) {
		case "SCRAM-SHA-256":
			sc.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
			sc.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
				return &xdgSCRAMClient{HashGeneratorFcn: SHA256}
			}
		case "SCRAM-SHA-512":
			sc.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
			sc.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
				return &xdgSCRAMClient{HashGeneratorFcn: SHA512}
			}
		default:
			sc.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		}
	}

	client, err := sarama.NewConsumerGroup(cfg.KafkaBrokers, cfg.KafkaGroupID, sc)
	if err != nil {
		return nil, err
	}

	return &Runner{
		client: client,
		group:  group,
		topics: cfg.Topics(),
		log:    log,
	}, nil
}

// Run запускает consume loop. Блокирует до отмены ctx.
func (r *Runner) Run(ctx context.Context) error {
	r.log.Info("consumer.runner.start",
		"topics", r.topics,
	)

	for {
		if err := r.client.Consume(ctx, r.topics, r.group); err != nil {
			r.log.Error("consumer.runner.error", "err", err)
		}

		if ctx.Err() != nil {
			r.log.Info("consumer.runner.stop", "reason", ctx.Err())
			return r.client.Close()
		}
	}
}