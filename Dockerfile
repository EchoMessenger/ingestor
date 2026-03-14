FROM golang:1.22-alpine AS builder

RUN apk add --no-cache git ca-certificates tzdata

WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build \
    -trimpath \
    -ldflags="-s -w -X main.version=${VERSION} -X main.buildTime=${BUILD_TIME}" \
    -o /bin/ingestor \
    ./cmd/main.go

# scratch — минимальный образ, нет shell, нет пакетного менеджера
FROM scratch

# Копируем только необходимое из builder
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo
COPY --from=builder /bin/ingestor /ingestor

# Порт health/readiness/metrics
EXPOSE 8081

ENTRYPOINT ["/ingestor"]