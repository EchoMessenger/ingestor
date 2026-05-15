//go:build integration

package integration

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	chclient "github.com/EchoMessenger/ingestor/internal/clickhouse"
	"github.com/EchoMessenger/ingestor/internal/config"
	"github.com/EchoMessenger/ingestor/internal/handler"
	pbx "github.com/EchoMessenger/ingestor/pbx"
	"github.com/IBM/sarama"
	"google.golang.org/protobuf/proto"
)

func TestAccountHandlerWritesToClickHouse(t *testing.T) {
	if os.Getenv("RUN_INGESTOR_INTEGRATION") != "1" {
		t.Skip("set RUN_INGESTOR_INTEGRATION=1 and start ingestor docker compose to run")
	}

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	ch, err := chclient.NewClient(cfg.ClickHouseAddr, cfg.ClickHouseDB, cfg.ClickHouseUser, cfg.ClickHousePassword)
	if err != nil {
		t.Fatalf("clickhouse: %v", err)
	}
	defer ch.Close()

	ctx := context.Background()
	if err := ch.Conn().Exec(ctx, `CREATE TABLE IF NOT EXISTS account_log (
		log_id UUID,
		log_timestamp DateTime64(3),
		action String,
		user_id String,
		default_acs_auth Nullable(String),
		default_acs_anon Nullable(String),
		public Nullable(String),
		tags Array(String)
	) ENGINE = MergeTree()
	ORDER BY (log_timestamp, user_id)`); err != nil {
		t.Fatalf("create table: %v", err)
	}

	registry, err := handler.NewRegistry(&config.Config{
		KafkaTopicPrefix: "tinode",
		BatchSize:        1,
		BatchTimeout:     time.Hour,
		RetryMax:         0,
		RetryBaseDelay:   time.Millisecond,
	}, ch, nil, slog.Default())
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	defer registry.StopAll()

	h, ok := registry.Get("tinode.account-events")
	if !ok {
		t.Fatalf("account handler not registered")
	}
	value, err := proto.Marshal(&pbx.AccountEvent{
		Action: pbx.Crud_CREATE,
		UserId: "integration-user",
		Public: []byte(`{"fn":"Integration"}`),
		Tags:   []string{"integration"},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := h.Handle(&sarama.ConsumerMessage{Value: value, Timestamp: time.Now()}); err != nil {
		t.Fatalf("handle: %v", err)
	}
	h.Stop()

	var count uint64
	if err := ch.Conn().QueryRow(ctx, `SELECT count() FROM account_log WHERE user_id = 'integration-user'`).Scan(&count); err != nil {
		t.Fatalf("query: %v", err)
	}
	if count == 0 {
		t.Fatalf("expected account_log row")
	}
}
