# ingestor

Kafka → ClickHouse ingestor для аудит-сервиса EchoMessenger.

Читает protobuf-события из Kafka топиков Tinode, десериализует их и пишет батчами в ClickHouse.

## GitHub Actions SonarQube

Для workflow `/.github/workflows/sonar.yml` нужны:
- `secrets.SONAR_HOST_URL`
- `secrets.SONAR_TOKEN`
- `vars.SONAR_PROJECT_KEY_INGESTOR`

## Архитектура

```
Kafka topics                    ClickHouse tables
─────────────────────────────   ──────────────────────
tinode.account-events        →  account_log
tinode.topic-events          →  topic_log
tinode.subscription-events   →  subscription_log
tinode.message-events        →  message_log
tinode.search-queries        →  search_log *
tinode.firehose.*  (9 топиков) → client_req_log
tinode.dlq                      ← ошибки десериализации и вставки
```

\* `search_log` заполняется только если в `tinode.conf` выставлен `"filters": { "find": true }`.
  См. `internal/handler/search.go`.

## Конфигурация

Все параметры через переменные окружения:

| Переменная | По умолчанию | Описание |
|---|---|---|
| `KAFKA_BROKERS` | `localhost:9092` | Брокеры через запятую |
| `KAFKA_GROUP_ID` | `ingestor` | Consumer group ID |
| `KAFKA_TOPIC_PREFIX` | `tinode` | Префикс топиков |
| `KAFKA_TLS_ENABLE` | `false` | |
| `KAFKA_SASL_ENABLE` | `false` | |
| `KAFKA_SASL_MECHANISM` | `PLAIN` | `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512` |
| `KAFKA_SASL_USERNAME` | | |
| `KAFKA_SASL_PASSWORD` | | Передавать через Secret, не ConfigMap |
| `CLICKHOUSE_ADDR` | `localhost:9000` | Native protocol (не HTTP) |
| `CLICKHOUSE_DB` | `default` | |
| `CLICKHOUSE_USER` | `default` | |
| `CLICKHOUSE_PASSWORD` | | Передавать через Secret |
| `BATCH_SIZE` | `1000` | Flush по достижении N строк |
| `BATCH_TIMEOUT` | `5s` | Flush по таймеру |
| `RETRY_MAX` | `3` | Попыток вставки перед DLQ |
| `RETRY_BASE_DELAY` | `500ms` | Начальная задержка retry (exponential backoff) |
| `DLQ_TOPIC` | `tinode.dlq` | Топик для failed сообщений |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error` |
| `HEALTH_ADDR` | `:8081` | Адрес health/metrics HTTP сервера |

## HTTP endpoints

| Path | Описание | k8s probe |
|---|---|---|
| `/healthz` | Liveness: ping ClickHouse | `livenessProbe` |
| `/readyz` | Readiness: consumer group готова | `readinessProbe` |
| `/metrics` | Prometheus метрики | `prometheus.io/scrape` |
| `/stats` | JSON статистика | ручная диагностика |

## Локальная разработка

```bash
# Поднять Kafka + ClickHouse, создать топики и таблицы
make infra-up

# Собрать
make build

# Запустить (читает .env.local)
make run

# Положить тестовые сообщения в каждый топик
go run ./cmd/seed/main.go

# Проверить health
make health
make stats
```

## k8s развёртывание

Сервис ожидает:
- `ConfigMap` с переменными окружения (кроме паролей)
- `Secret` с `KAFKA_SASL_PASSWORD` и `CLICKHOUSE_PASSWORD`

## Таблицы ClickHouse

DDL находится в `deploy/clickhouse-init/01_tables.sql`.

При локальной разработке выполняется автоматически через `docker-entrypoint-initdb.d`.
В production — применять через миграции или вручную перед первым деплоем.
