package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	chclient "github.com/EchoMessenger/ingestor/internal/clickhouse"
	"github.com/EchoMessenger/ingestor/internal/config"
	"github.com/EchoMessenger/ingestor/internal/consumer"
	"github.com/EchoMessenger/ingestor/internal/dlq"
	"github.com/EchoMessenger/ingestor/internal/handler"
)

func main() {
	startTime := time.Now()

	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "config load failed: %v\n", err)
		os.Exit(1)
	}

	level := slog.LevelInfo
	switch strings.ToLower(cfg.LogLevel) {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}

	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(log)

	log.Info("service.start",
		"pid", os.Getpid(),
		"log_level", level.String(),
		"kafka_brokers", cfg.KafkaBrokers,
		"kafka_group", cfg.KafkaGroupID,
		"topic_prefix", cfg.KafkaTopicPrefix,
		"clickhouse_addr", cfg.ClickHouseAddr,
		"clickhouse_db", cfg.ClickHouseDB,
		"batch_size", cfg.BatchSize,
		"batch_timeout", cfg.BatchTimeout.String(),
		"retry_max", cfg.RetryMax,
	)

	// ---- ClickHouse ----
	ch, err := chclient.NewClient(cfg.ClickHouseAddr, cfg.ClickHouseDB, cfg.ClickHouseUser, cfg.ClickHousePassword)
	if err != nil {
		log.Error("clickhouse.init.failed", "err", err)
		os.Exit(1)
	}
	log.Info("clickhouse.init.ok", "addr", cfg.ClickHouseAddr)

	// ---- DLQ producer ----
	dlqProducer, err := dlq.NewProducer(cfg.KafkaBrokers, cfg.DLQTopic, log)
	if err != nil {
		log.Error("dlq.init.failed", "err", err)
		os.Exit(1)
	}
	log.Info("dlq.init.ok", "topic", cfg.DLQTopic)

	dlqFn := func(sourceTopic, key string, value []byte, reason string) {
		dlqProducer.Send(sourceTopic, key, value, reason)
	}

	// ---- Handler registry ----
	registry, err := handler.NewRegistry(cfg, ch, dlqFn, log)
	if err != nil {
		log.Error("handler.registry.init.failed", "err", err)
		os.Exit(1)
	}
	log.Info("handler.registry.init.ok")

	// ---- Consumer group ----
	grp := consumer.NewGroup(registry, dlqFn, log)
	runner, err := consumer.NewRunner(cfg, grp, log)
	if err != nil {
		log.Error("consumer.runner.init.failed", "err", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())

	runErr := make(chan error, 1)
	go func() {
		runErr <- runner.Run(ctx)
	}()

	// ---- Health + Readiness HTTP ----
	//
	// /healthz  — liveness probe:  сервис жив если ClickHouse отвечает на ping.
	//             k8s перезапустит pod если этот endpoint вернёт не 200.
	//
	// /readyz   — readiness probe: сервис готов если consumer group установила
	//             подписки (ready=1). До этого момента pod исключается из балансировки.
	//             Для consumer'а это означает что он не будет получать partition'ы
	//             пока не готов их обрабатывать.
	//
	// /metrics  — текстовый endpoint для Prometheus (prometheus.io/scrape: "true")
	//
	// /stats    — JSON endpoint для ручной диагностики

	var ready atomic.Int32 // 0 = not ready, 1 = ready

	// Consumer сигнализирует о готовности через callback после Setup()
	grp.OnReady(func() {
		ready.Store(1)
		log.Info("service.ready_probe.ok")
	})

	mux := http.NewServeMux()

	// Liveness: проверяем что ClickHouse отвечает
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if err := ch.Conn().Ping(r.Context()); err != nil {
			log.Warn("healthz.clickhouse.ping_failed", "err", err)
			http.Error(w, "clickhouse unreachable", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	// Readiness: consumer group установила подписки
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if ready.Load() == 0 {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	// Stats: JSON для ручной диагностики
	mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		type stats struct {
			UptimeSeconds int    `json:"uptime_seconds"`
			Ready         bool   `json:"ready"`
			KafkaGroup    string `json:"kafka_group"`
			Topics        int    `json:"topics_count"`
		}
		s := stats{
			UptimeSeconds: int(time.Since(startTime).Seconds()),
			Ready:         ready.Load() == 1,
			KafkaGroup:    cfg.KafkaGroupID,
			Topics:        len(cfg.Topics()),
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(s)
	})

	// Metrics: Prometheus-совместимый текстовый формат
	// Минимальный набор без внешних зависимостей.
	// Если понадобится полный prometheus/client_golang — заменить этот handler.
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprintf(w, "# HELP ingestor_uptime_seconds Seconds since service start\n")
		fmt.Fprintf(w, "# TYPE ingestor_uptime_seconds gauge\n")
		fmt.Fprintf(w, "ingestor_uptime_seconds %d\n", int(time.Since(startTime).Seconds()))
		fmt.Fprintf(w, "# HELP ingestor_ready Whether the consumer group is ready\n")
		fmt.Fprintf(w, "# TYPE ingestor_ready gauge\n")
		fmt.Fprintf(w, "ingestor_ready %d\n", ready.Load())
	})

	go func() {
		log.Info("health.listen.ok", "addr", cfg.HealthAddr)
		if err := http.ListenAndServe(cfg.HealthAddr, mux); err != nil {
			log.Error("health.serve.failed", "err", err)
		}
	}()

	log.Info("service.ready",
		"topics", cfg.Topics(),
		"health_addr", cfg.HealthAddr,
	)

	// ---- Graceful shutdown ----
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	select {
	case s := <-sig:
		log.Info("service.shutdown.start", "signal", s.String())
	case err := <-runErr:
		log.Error("service.shutdown.consumer_error", "err", err)
	}

	// 1. Останавливаем приём новых сообщений
	cancel()

	// 2. Ждём завершения consumer loop
	select {
	case <-runErr:
		log.Info("service.shutdown.consumer.ok")
	case <-time.After(15 * time.Second):
		log.Warn("service.shutdown.consumer.timeout")
	}

	// 3. Финальный flush всех batch'ей — данные не теряются
	log.Info("service.shutdown.flushing_batches")
	registry.StopAll()
	log.Info("service.shutdown.batches.ok")

	// 4. Закрываем соединения
	dlqProducer.Close()
	ch.Close()

	log.Info("service.shutdown.complete",
		"uptime_seconds", int(time.Since(startTime).Seconds()),
	)
}