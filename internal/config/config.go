package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	// Kafka
	KafkaBrokers       []string
	KafkaGroupID       string
	KafkaTopicPrefix   string
	KafkaTLSEnable     bool
	KafkaSASLEnable    bool
	KafkaSASLMechanism string
	KafkaSASLUsername  string
	KafkaSASLPassword  string

	// ClickHouse
	ClickHouseAddr     string
	ClickHouseDB       string
	ClickHouseUser     string
	ClickHousePassword string

	// Batch
	BatchSize          int
	BatchTimeout       time.Duration

	// Retry
	RetryMax           int
	RetryBaseDelay     time.Duration

	// DLQ
	DLQTopic string

	// Service
	LogLevel   string
	HealthAddr string
}

func Load() (*Config, error) {
	cfg := &Config{
		KafkaBrokers:       splitEnv("KAFKA_BROKERS", "localhost:9092"),
		KafkaGroupID:       getEnv("KAFKA_GROUP_ID", "ingestor"),
		KafkaTopicPrefix:   getEnv("KAFKA_TOPIC_PREFIX", "tinode"),
		KafkaTLSEnable:     getBoolEnv("KAFKA_TLS_ENABLE", false),
		KafkaSASLEnable:    getBoolEnv("KAFKA_SASL_ENABLE", false),
		KafkaSASLMechanism: getEnv("KAFKA_SASL_MECHANISM", "PLAIN"),
		KafkaSASLUsername:  getEnv("KAFKA_SASL_USERNAME", ""),
		KafkaSASLPassword:  getEnv("KAFKA_SASL_PASSWORD", ""),

		ClickHouseAddr:     getEnv("CLICKHOUSE_ADDR", "localhost:9000"),
		ClickHouseDB:       getEnv("CLICKHOUSE_DB", "default"),
		ClickHouseUser:     getEnv("CLICKHOUSE_USER", "default"),
		ClickHousePassword: getEnv("CLICKHOUSE_PASSWORD", ""),

		BatchSize:    getIntEnv("BATCH_SIZE", 1000),
		BatchTimeout: getDurationEnv("BATCH_TIMEOUT", 5*time.Second),

		RetryMax:       getIntEnv("RETRY_MAX", 3),
		RetryBaseDelay: getDurationEnv("RETRY_BASE_DELAY", 500*time.Millisecond),

		DLQTopic: getEnv("DLQ_TOPIC", "tinode.dlq"),

		LogLevel:   getEnv("LOG_LEVEL", "info"),
		HealthAddr: getEnv("HEALTH_ADDR", ":8081"),
	}

	if len(cfg.KafkaBrokers) == 0 {
		return nil, fmt.Errorf("KAFKA_BROKERS is required")
	}

	return cfg, nil
}

// Topics возвращает полные имена всех топиков которые слушает ingestor.
//
// tinode.search-queries включён в список подписки, но будет пустым
// пока в tinode.conf не выставлен "filters": { "find": true }.
// Подписка на пустой топик безопасна — consumer просто не получает сообщений.
func (c *Config) Topics() []string {
	prefix := strings.TrimSuffix(c.KafkaTopicPrefix, ".")
	names := []string{
		"account-events",
		"topic-events",
		"subscription-events",
		"message-events",
		"search-queries",
		"firehose.handshake",
		"firehose.account-mgmt",
		"firehose.auth",
		"firehose.subscriptions",
		"firehose.messages",
		"firehose.queries",
		"firehose.updates",
		"firehose.deletions",
		"firehose.notifications",
	}
	topics := make([]string, len(names))
	for i, n := range names {
		topics[i] = prefix + "." + n
	}
	return topics
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func splitEnv(key, def string) []string {
	v := getEnv(key, def)
	return strings.Split(v, ",")
}

func getBoolEnv(key string, def bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return def
	}
	return b
}

func getIntEnv(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	i, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return i
}

func getDurationEnv(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return d
}