package config

import (
	"reflect"
	"testing"
	"time"
)

func TestLoadReadsEnvironmentAndDefaults(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "k1:9092,k2:9092")
	t.Setenv("KAFKA_GROUP_ID", "group")
	t.Setenv("KAFKA_TOPIC_PREFIX", "audit.")
	t.Setenv("KAFKA_TLS_ENABLE", "true")
	t.Setenv("KAFKA_SASL_ENABLE", "true")
	t.Setenv("KAFKA_SASL_MECHANISM", "SCRAM-SHA-512")
	t.Setenv("KAFKA_SASL_USERNAME", "user")
	t.Setenv("KAFKA_SASL_PASSWORD", "pass")
	t.Setenv("CLICKHOUSE_ADDR", "clickhouse:9000")
	t.Setenv("CLICKHOUSE_DB", "audit")
	t.Setenv("BATCH_SIZE", "50")
	t.Setenv("BATCH_TIMEOUT", "2s")
	t.Setenv("RETRY_MAX", "4")
	t.Setenv("RETRY_BASE_DELAY", "10ms")
	t.Setenv("DLQ_TOPIC", "audit.dlq")
	t.Setenv("LOG_LEVEL", "debug")
	t.Setenv("HEALTH_ADDR", ":9090")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if !reflect.DeepEqual(cfg.KafkaBrokers, []string{"k1:9092", "k2:9092"}) {
		t.Fatalf("KafkaBrokers = %#v", cfg.KafkaBrokers)
	}
	if cfg.KafkaGroupID != "group" || cfg.KafkaTopicPrefix != "audit." {
		t.Fatalf("unexpected kafka cfg: %#v", cfg)
	}
	if !cfg.KafkaTLSEnable || !cfg.KafkaSASLEnable || cfg.KafkaSASLMechanism != "SCRAM-SHA-512" {
		t.Fatalf("unexpected security cfg: %#v", cfg)
	}
	if cfg.ClickHouseAddr != "clickhouse:9000" || cfg.ClickHouseDB != "audit" {
		t.Fatalf("unexpected clickhouse cfg: %#v", cfg)
	}
	if cfg.BatchSize != 50 || cfg.BatchTimeout != 2*time.Second || cfg.RetryMax != 4 || cfg.RetryBaseDelay != 10*time.Millisecond {
		t.Fatalf("unexpected batch/retry cfg: %#v", cfg)
	}
	if cfg.DLQTopic != "audit.dlq" || cfg.LogLevel != "debug" || cfg.HealthAddr != ":9090" {
		t.Fatalf("unexpected service cfg: %#v", cfg)
	}
}

func TestTopicsUsesTrimmedPrefix(t *testing.T) {
	cfg := &Config{KafkaTopicPrefix: "tinode."}
	got := cfg.Topics()
	want := []string{
		"tinode.account-events",
		"tinode.topic-events",
		"tinode.subscription-events",
		"tinode.message-events",
		"tinode.search-queries",
		"tinode.firehose.handshake",
		"tinode.firehose.account-mgmt",
		"tinode.firehose.auth",
		"tinode.firehose.subscriptions",
		"tinode.firehose.messages",
		"tinode.firehose.queries",
		"tinode.firehose.updates",
		"tinode.firehose.deletions",
		"tinode.firehose.notifications",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Topics() = %#v, want %#v", got, want)
	}
}

func TestInvalidEnvFallsBackToDefaults(t *testing.T) {
	t.Setenv("KAFKA_TLS_ENABLE", "not-bool")
	t.Setenv("BATCH_SIZE", "not-int")
	t.Setenv("BATCH_TIMEOUT", "not-duration")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if cfg.KafkaTLSEnable {
		t.Fatalf("KafkaTLSEnable = true, want default false")
	}
	if cfg.BatchSize != 1000 || cfg.BatchTimeout != 5*time.Second {
		t.Fatalf("invalid env did not fall back to defaults: %#v", cfg)
	}
}
