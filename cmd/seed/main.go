// cmd/seed/main.go
//
// Утилита для ручного тестирования ingestor.
// Кладёт по одному реальному protobuf-сообщению в каждый Kafka топик.
//
// Запуск:
//
//	go run ./cmd/seed/main.go
//
// Или с кастомными брокерами:
//
//	KAFKA_BROKERS=localhost:9092 go run ./cmd/seed/main.go
package main

import (
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	pbx "github.com/EchoMessenger/ingestor/pbx"
	"github.com/IBM/sarama"
	"google.golang.org/protobuf/proto"
)

const prefix = "tinode"

func main() {
	brokers := strings.Split(getEnv("KAFKA_BROKERS", "localhost:9092"), ",")

	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	cfg.Producer.RequiredAcks = sarama.WaitForAll

	prod, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		log.Fatalf("producer init failed: %v", err)
	}
	defer prod.Close()

	log.Printf("connected to kafka: %v", brokers)

	send := func(topic, key string, msg proto.Message) {
		val, err := proto.Marshal(msg)
		if err != nil {
			log.Fatalf("marshal failed for %s: %v", topic, err)
		}
		fullTopic := prefix + "." + topic
		_, _, err = prod.SendMessage(&sarama.ProducerMessage{
			Topic: fullTopic,
			Key:   sarama.StringEncoder(key),
			Value: sarama.ByteEncoder(val),
		})
		if err != nil {
			log.Fatalf("send failed for %s: %v", fullTopic, err)
		}
		log.Printf("✓ %-40s key=%-20s bytes=%d", fullTopic, key, len(val))
	}

	now := time.Now().UnixMilli()

	// ----------------------------------------------------------------
	// 1. account-events → account_log
	// ----------------------------------------------------------------
	send("account-events", "usr001", &pbx.AccountEvent{
		Action: pbx.Crud_CREATE,
		UserId: "usr001",
		DefaultAcs: &pbx.DefaultAcsMode{
			Auth: "JRWPA",
			Anon: "N",
		},
		Public: []byte(`{"fn":"Alice Tester"}`),
		Tags:   []string{"email:alice@example.com"},
	})

	// ----------------------------------------------------------------
	// 2. topic-events → topic_log
	// ----------------------------------------------------------------
	send("topic-events", "grpABC123", &pbx.TopicEvent{
		Action: pbx.Crud_CREATE,
		Name:   "grpABC123",
		Desc: &pbx.TopicDesc{
			CreatedAt: now,
			UpdatedAt: now,
			SeqId:     0,
			State:     "ok",
			IsChan:    false,
			Online:    true,
			Defacs: &pbx.DefaultAcsMode{
				Auth: "JRWPA",
				Anon: "N",
			},
			Public: []byte(`{"fn":"Test Group"}`),
		},
	})

	// ----------------------------------------------------------------
	// 3. subscription-events → subscription_log
	// ----------------------------------------------------------------
	send("subscription-events", "grpABC123", &pbx.SubscriptionEvent{
		Action: pbx.Crud_CREATE,
		Topic:  "grpABC123",
		UserId: "usr001",
		Mode: &pbx.AccessMode{
			Want:  "JRWPA",
			Given: "JRWPA",
		},
		ReadId: 0,
		RecvId: 0,
	})

	// ----------------------------------------------------------------
	// 4. message-events → message_log
	// ----------------------------------------------------------------
	send("message-events", "grpABC123", &pbx.MessageEvent{
		Action: pbx.Crud_CREATE,
		Msg: &pbx.ServerData{
			Topic:      "grpABC123",
			FromUserId: "usr001",
			Timestamp:  now,
			SeqId:      1,
			Content:    []byte(`{"txt":"Hello, audit!"}`),
			Head: map[string][]byte{
				"mime": []byte("text/x-drafty"),
			},
		},
	})

	// ----------------------------------------------------------------
	// 5. search-queries → search_log
	//    (топик пуст если find_handler выключен — см. internal/handler/search.go)
	// ----------------------------------------------------------------
	send("search-queries", "usr001", &pbx.SearchQuery{
		UserId: "usr001",
		Query:  "alice",
	})

	// ----------------------------------------------------------------
	// 6. firehose.handshake → client_req_log (msg_type=HI)
	// ----------------------------------------------------------------
	send("firehose.handshake", "usr001", &pbx.ClientReq{
		Sess: testSess("usr001", "sess-hi-001"),
		Msg: &pbx.ClientMsg{
			Message: &pbx.ClientMsg_Hi{
				Hi: &pbx.ClientHi{
					Id:        "req001",
					UserAgent: "TinodeWeb/0.22",
					Ver:       "0.22.0",
					DeviceId:  "dev-mac-001",
					Lang:      "en-US",
					Platform:  "web",
				},
			},
		},
	})

	// ----------------------------------------------------------------
	// 7. firehose.auth → client_req_log (msg_type=LOGIN)
	//    Secret намеренно пустой — зачищается в router перед отправкой
	// ----------------------------------------------------------------
	send("firehose.auth", "usr001", &pbx.ClientReq{
		Sess: testSess("usr001", "sess-login-001"),
		Msg: &pbx.ClientMsg{
			Message: &pbx.ClientMsg_Login{
				Login: &pbx.ClientLogin{
					Id:     "req002",
					Scheme: "basic",
					// Secret: nil — зачищен в router (redactSecrets)
				},
			},
		},
	})

	// ----------------------------------------------------------------
	// 8. firehose.account-mgmt → client_req_log (msg_type=ACC)
	// ----------------------------------------------------------------
	send("firehose.account-mgmt", "usr002", &pbx.ClientReq{
		Sess: testSess("usr002", "sess-acc-001"),
		Msg: &pbx.ClientMsg{
			Message: &pbx.ClientMsg_Acc{
				Acc: &pbx.ClientAcc{
					Id:     "req003",
					Scheme: "basic",
					Login:  true,
					State:  "ok",
					Tags:   []string{"email:bob@example.com"},
					// Secret: nil — зачищен в router
				},
			},
		},
	})

	// ----------------------------------------------------------------
	// 9. firehose.subscriptions → client_req_log (msg_type=SUB)
	// ----------------------------------------------------------------
	send("firehose.subscriptions", "grpABC123", &pbx.ClientReq{
		Sess: testSess("usr001", "sess-sub-001"),
		Msg: &pbx.ClientMsg{
			Message: &pbx.ClientMsg_Sub{
				Sub: &pbx.ClientSub{
					Id:    "req004",
					Topic: "grpABC123",
				},
			},
		},
	})

	// ----------------------------------------------------------------
	// 10. firehose.messages → client_req_log (msg_type=PUB)
	// ----------------------------------------------------------------
	send("firehose.messages", "grpABC123", &pbx.ClientReq{
		Sess: testSess("usr001", "sess-pub-001"),
		Msg: &pbx.ClientMsg{
			Message: &pbx.ClientMsg_Pub{
				Pub: &pbx.ClientPub{
					Id:      "req005",
					Topic:   "grpABC123",
					Content: []byte(`{"txt":"Hello from seed!"}`),
					Head: map[string][]byte{
						"mime": []byte("text/x-drafty"),
					},
				},
			},
		},
	})

	// ----------------------------------------------------------------
	// 11. firehose.queries → client_req_log (msg_type=GET)
	// ----------------------------------------------------------------
	send("firehose.queries", "usr001", &pbx.ClientReq{
		Sess: testSess("usr001", "sess-get-001"),
		Msg: &pbx.ClientMsg{
			Message: &pbx.ClientMsg_Get{
				Get: &pbx.ClientGet{
					Id:    "req006",
					Topic: "grpABC123",
					Query: &pbx.GetQuery{What: "desc"},
				},
			},
		},
	})

	// ----------------------------------------------------------------
	// 12. firehose.updates → client_req_log (msg_type=SET)
	// ----------------------------------------------------------------
	send("firehose.updates", "grpABC123", &pbx.ClientReq{
		Sess: testSess("usr001", "sess-set-001"),
		Msg: &pbx.ClientMsg{
			Message: &pbx.ClientMsg_Set{
				Set: &pbx.ClientSet{
					Id:    "req007",
					Topic: "grpABC123",
					Query: &pbx.SetQuery{
						Tags: []string{"important"},
					},
				},
			},
		},
	})

	// ----------------------------------------------------------------
	// 13. firehose.deletions → client_req_log (msg_type=DEL)
	// ----------------------------------------------------------------
	send("firehose.deletions", "grpABC123", &pbx.ClientReq{
		Sess: testSess("usr001", "sess-del-001"),
		Msg: &pbx.ClientMsg{
			Message: &pbx.ClientMsg_Del{
				Del: &pbx.ClientDel{
					Id:    "req008",
					Topic: "grpABC123",
					What:  pbx.ClientDel_MSG,
					Hard:  false,
					DelSeq: []*pbx.SeqRange{
						{Low: 1, Hi: 1},
					},
				},
			},
		},
	})

	// ----------------------------------------------------------------
	// 14. firehose.notifications → client_req_log (msg_type=NOTE)
	// ----------------------------------------------------------------
	send("firehose.notifications", "grpABC123", &pbx.ClientReq{
		Sess: testSess("usr001", "sess-note-001"),
		Msg: &pbx.ClientMsg{
			Message: &pbx.ClientMsg_Note{
				Note: &pbx.ClientNote{
					Topic: "grpABC123",
					What:  pbx.InfoNote_READ,
					SeqId: 1,
				},
			},
		},
	})

	fmt.Println("\n✓ All messages sent. Wait ~5s for batch flush, then check ClickHouse:")
	fmt.Println()
	fmt.Println("  docker compose exec clickhouse clickhouse-client")
	fmt.Println()
	fmt.Println("  SELECT count() FROM account_log;")
	fmt.Println("  SELECT count() FROM topic_log;")
	fmt.Println("  SELECT count() FROM subscription_log;")
	fmt.Println("  SELECT count() FROM message_log;")
	fmt.Println("  SELECT count() FROM search_log;")
	fmt.Println("  SELECT count() FROM client_req_log;")
	fmt.Println()
	fmt.Println("  -- Детали по каждой таблице:")
	fmt.Println("  SELECT log_timestamp, action, user_id FROM account_log;")
	fmt.Println("  SELECT log_timestamp, action, topic_name FROM topic_log;")
	fmt.Println("  SELECT log_timestamp, action, topic, user_id FROM subscription_log;")
	fmt.Println("  SELECT log_timestamp, action, msg_topic, msg_seq_id, msg_content FROM message_log;")
	fmt.Println("  SELECT log_timestamp, user_id, query FROM search_log;")
	fmt.Println("  SELECT log_timestamp, msg_type, sess_user_id, msg_topic FROM client_req_log ORDER BY log_timestamp;")
}

// testSess создаёт тестовую сессию.
func testSess(userID, sessionID string) *pbx.Session {
	return &pbx.Session{
		SessionId:  sessionID,
		UserId:     userID,
		AuthLevel:  pbx.AuthLevel_AUTH,
		RemoteAddr: "127.0.0.1",
		UserAgent:  "TinodeWeb/0.22 (test seed)",
		DeviceId:   "dev-seed-001",
		Language:   "en-US",
	}
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}