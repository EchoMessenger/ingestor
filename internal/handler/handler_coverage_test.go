package handler

import (
	"testing"
	"time"

	chclient "github.com/EchoMessenger/ingestor/internal/clickhouse"
	pbx "github.com/EchoMessenger/ingestor/pbx"
	"github.com/IBM/sarama"
)

// TestAccountHandlerEdgeCases covers account handler with different field combinations
func TestAccountHandlerEdgeCases(t *testing.T) {
	tests := []struct {
		name  string
		event *pbx.AccountEvent
		check func(*testing.T, accountRow)
	}{
		{
			name: "without_default_acs",
			event: &pbx.AccountEvent{
				Action:     pbx.Crud_DELETE,
				UserId:     "user123",
				DefaultAcs: nil,
				Public:     []byte(`{}`),
				Tags:       []string{"tag1", "tag2"},
			},
			check: func(t *testing.T, row accountRow) {
				if row.DefaultAcsAuth != nil || row.DefaultAcsAnon != nil {
					t.Fatalf("expected nil DefaultAcs fields, got auth=%v, anon=%v", row.DefaultAcsAuth, row.DefaultAcsAnon)
				}
				if len(row.Tags) != 2 {
					t.Fatalf("expected 2 tags, got %d", len(row.Tags))
				}
			},
		},
		{
			name: "without_public",
			event: &pbx.AccountEvent{
				Action:     pbx.Crud_UPDATE,
				UserId:     "user456",
				DefaultAcs: &pbx.DefaultAcsMode{Auth: "JRWPA", Anon: "N"},
				Public:     nil,
			},
			check: func(t *testing.T, row accountRow) {
				if row.Public != nil {
					t.Fatalf("expected nil Public field")
				}
				if *row.DefaultAcsAuth != "JRWPA" {
					t.Fatalf("expected JRWPA, got %s", *row.DefaultAcsAuth)
				}
			},
		},
		{
			name: "empty_tags",
			event: &pbx.AccountEvent{
				Action: pbx.Crud_CREATE,
				UserId: "user789",
				Tags:   []string{},
			},
			check: func(t *testing.T, row accountRow) {
				if len(row.Tags) != 0 {
					t.Fatalf("expected empty tags slice")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var rows []chclient.Row
			h := newAccountHandler("tinode", nil, recordingBatch(&rows), logger())

			if err := h.Handle(&sarama.ConsumerMessage{
				Value:     marshal(t, tt.event),
				Timestamp: time.UnixMilli(1700000000123),
			}); err != nil {
				t.Fatalf("Handle failed: %v", err)
			}
			h.Stop()

			if len(rows) != 1 {
				t.Fatalf("expected 1 row, got %d", len(rows))
			}
			tt.check(t, rows[0].(accountRow))
		})
	}
}

// TestTopicHandlerEdgeCases covers topic handler with different desc combinations
func TestTopicHandlerEdgeCases(t *testing.T) {
	tests := []struct {
		name  string
		event *pbx.TopicEvent
		check func(*testing.T, topicRow)
	}{
		{
			name: "without_desc",
			event: &pbx.TopicEvent{
				Action: pbx.Crud_CREATE,
				Name:   "grp123",
				Desc:   nil,
			},
			check: func(t *testing.T, row topicRow) {
				if row.DescCreatedAt != nil || row.DescIsChan != nil || row.DescOnline != nil {
					t.Fatalf("expected all nil Desc fields")
				}
			},
		},
		{
			name: "without_defacs_and_acs",
			event: &pbx.TopicEvent{
				Action: pbx.Crud_UPDATE,
				Name:   "grp456",
				Desc: &pbx.TopicDesc{
					CreatedAt: 100,
					Defacs:    nil,
					Acs:       nil,
				},
			},
			check: func(t *testing.T, row topicRow) {
				if row.DescDefacsAuth != nil || row.DescDefacsAnon != nil {
					t.Fatalf("expected nil Defacs fields")
				}
				if row.DescAcsWant != nil || row.DescAcsGiven != nil {
					t.Fatalf("expected nil Acs fields")
				}
			},
		},
		{
			name: "boolean_false_values",
			event: &pbx.TopicEvent{
				Action: pbx.Crud_CREATE,
				Name:   "grp789",
				Desc: &pbx.TopicDesc{
					IsChan: false,
					Online: false,
				},
			},
			check: func(t *testing.T, row topicRow) {
				if row.DescIsChan == nil || *row.DescIsChan {
					t.Fatalf("expected DescIsChan to be false")
				}
				if row.DescOnline == nil || *row.DescOnline {
					t.Fatalf("expected DescOnline to be false")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var rows []chclient.Row
			h := newTopicHandler("tinode", nil, recordingBatch(&rows), logger())

			if err := h.Handle(&sarama.ConsumerMessage{
				Value:     marshal(t, tt.event),
				Timestamp: time.UnixMilli(1700000000123),
			}); err != nil {
				t.Fatalf("Handle failed: %v", err)
			}
			h.Stop()

			if len(rows) != 1 {
				t.Fatalf("expected 1 row, got %d", len(rows))
			}
			tt.check(t, rows[0].(topicRow))
		})
	}
}

// TestSubscriptionHandlerEdgeCases covers subscription handler with different combinations
func TestSubscriptionHandlerEdgeCases(t *testing.T) {
	tests := []struct {
		name  string
		event *pbx.SubscriptionEvent
		check func(*testing.T, subscriptionRow)
	}{
		{
			name: "without_mode",
			event: &pbx.SubscriptionEvent{
				Action: pbx.Crud_CREATE,
				Topic:  "grp789",
				UserId: "user789",
				Mode:   nil,
			},
			check: func(t *testing.T, row subscriptionRow) {
				if row.ModeWant != nil || row.ModeGiven != nil {
					t.Fatalf("expected nil Mode fields")
				}
			},
		},
		{
			name: "zero_id_values",
			event: &pbx.SubscriptionEvent{
				Action: pbx.Crud_UPDATE,
				Topic:  "grp999",
				UserId: "user999",
				DelId:  0,
				ReadId: 0,
				RecvId: 0,
			},
			check: func(t *testing.T, row subscriptionRow) {
				if row.DelID != nil || row.ReadID != nil || row.RecvID != nil {
					t.Fatalf("expected nil for zero values")
				}
			},
		},
		{
			name: "without_private",
			event: &pbx.SubscriptionEvent{
				Action: pbx.Crud_DELETE,
				Topic:  "grp111",
				UserId: "user111",
				Private: nil,
			},
			check: func(t *testing.T, row subscriptionRow) {
				if row.Private != nil {
					t.Fatalf("expected nil Private")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var rows []chclient.Row
			h := newSubscriptionHandler("tinode", nil, recordingBatch(&rows), logger())

			if err := h.Handle(&sarama.ConsumerMessage{
				Value:     marshal(t, tt.event),
				Timestamp: time.UnixMilli(1700000000123),
			}); err != nil {
				t.Fatalf("Handle failed: %v", err)
			}
			h.Stop()

			if len(rows) != 1 {
				t.Fatalf("expected 1 row, got %d", len(rows))
			}
			tt.check(t, rows[0].(subscriptionRow))
		})
	}
}

// TestMessageHandlerEdgeCases covers message handler with different combinations
func TestMessageHandlerEdgeCases(t *testing.T) {
	tests := []struct {
		name  string
		event *pbx.MessageEvent
		check func(*testing.T, messageRow)
	}{
		{
			name: "full_message",
			event: &pbx.MessageEvent{
				Action: pbx.Crud_CREATE,
				Msg: &pbx.ServerData{
					Topic:      "grp123",
					FromUserId: "user123",
					Timestamp:  1700000000000,
					DeletedAt:  0,
					SeqId:      42,
					Head: map[string][]byte{
						"mime": []byte("text/plain"),
						"ref":  []byte("reply-42"),
					},
					Content: []byte("Hello"),
				},
			},
			check: func(t *testing.T, row messageRow) {
				if len(row.MsgHead) != 2 {
					t.Fatalf("expected 2 head fields, got %d", len(row.MsgHead))
				}
				if row.MsgHead["mime"] != "text/plain" {
					t.Fatalf("expected mime text/plain")
				}
				if *row.MsgContent != "Hello" {
					t.Fatalf("expected Hello")
				}
			},
		},
		{
			name: "nullable_fields",
			event: &pbx.MessageEvent{
				Action: pbx.Crud_CREATE,
				Msg: &pbx.ServerData{
					Topic:      "grp456",
					FromUserId: "",
					Timestamp:  0,
					DeletedAt:  0,
					SeqId:      0,
					Content:    nil,
				},
			},
			check: func(t *testing.T, row messageRow) {
				if row.MsgFromUser != nil {
					t.Fatalf("expected nil FromUser for empty string")
				}
				if row.MsgDeletedAt != nil {
					t.Fatalf("expected nil DeletedAt for 0")
				}
				if row.MsgContent != nil {
					t.Fatalf("expected nil Content for nil")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var rows []chclient.Row
			h := newMessageHandler("tinode", nil, recordingBatch(&rows), logger())

			if err := h.Handle(&sarama.ConsumerMessage{
				Value:     marshal(t, tt.event),
				Timestamp: time.UnixMilli(1700000000123),
			}); err != nil {
				t.Fatalf("Handle failed: %v", err)
			}
			h.Stop()

			if len(rows) != 1 {
				t.Fatalf("expected 1 row, got %d", len(rows))
			}
			tt.check(t, rows[0].(messageRow))
		})
	}
}

// TestSearchHandlerEdgeCases covers search handler
func TestSearchHandlerEdgeCases(t *testing.T) {
	tests := []struct {
		name  string
		event *pbx.SearchQuery
		check func(*testing.T, searchRow)
	}{
		{
			name: "empty_query",
			event: &pbx.SearchQuery{
				UserId: "user123",
				Query:  "",
			},
			check: func(t *testing.T, row searchRow) {
				if row.Query != "" {
					t.Fatalf("expected empty query")
				}
			},
		},
		{
			name: "long_query",
			event: &pbx.SearchQuery{
				UserId: "user456",
				Query:  "this is a very long search query with many words",
			},
			check: func(t *testing.T, row searchRow) {
				if len(row.Query) != len("this is a very long search query with many words") {
					t.Fatalf("query length mismatch")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var rows []chclient.Row
			h := newSearchHandler("tinode", nil, recordingBatch(&rows), logger())

			if err := h.Handle(&sarama.ConsumerMessage{
				Value:     marshal(t, tt.event),
				Timestamp: time.UnixMilli(1700000000123),
			}); err != nil {
				t.Fatalf("Handle failed: %v", err)
			}
			h.Stop()

			if len(rows) != 1 {
				t.Fatalf("expected 1 row, got %d", len(rows))
			}
			tt.check(t, rows[0].(searchRow))
		})
	}
}
