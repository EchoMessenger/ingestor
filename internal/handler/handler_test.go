package handler

import (
	"context"
	"io"
	"log/slog"
	"reflect"
	"testing"
	"time"

	chclient "github.com/EchoMessenger/ingestor/internal/clickhouse"
	"github.com/EchoMessenger/ingestor/internal/config"
	pbx "github.com/EchoMessenger/ingestor/pbx"
	"github.com/IBM/sarama"
	"google.golang.org/protobuf/proto"
)

func logger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func marshal(t *testing.T, msg proto.Message) []byte {
	t.Helper()
	value, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return value
}

func msg(value []byte) *sarama.ConsumerMessage {
	return &sarama.ConsumerMessage{Key: []byte("key"), Value: value, Timestamp: time.UnixMilli(1700000000123)}
}

func recordingBatch(rows *[]chclient.Row) makeBatchFn {
	return func(name string, flushFn chclient.FlushFunc) *chclient.Batch {
		return chclient.NewBatch(name, 1, time.Hour, 0, time.Millisecond, func(ctx context.Context, batch []chclient.Row) error {
			*rows = append(*rows, batch...)
			return nil
		}, nil, logger())
	}
}

func TestHelpers(t *testing.T) {
	if got := topic("tinode", "message-events"); got != "tinode.message-events" {
		t.Fatalf("topic = %q", got)
	}
	if nullableString("") != nil || *nullableString("x") != "x" {
		t.Fatalf("nullableString mismatch")
	}
	if nullableInt32(0) != nil || *nullableInt32(7) != 7 {
		t.Fatalf("nullableInt32 mismatch")
	}
	if nullableInt64(0) != nil || *nullableInt64(9) != 9 {
		t.Fatalf("nullableInt64 mismatch")
	}
	if b := nullableBool(false); b == nil || *b {
		t.Fatalf("nullableBool(false) = %#v", b)
	}
	if got := mapBytesToString(map[string][]byte{"a": []byte("b")}); !reflect.DeepEqual(got, map[string]string{"a": "b"}) {
		t.Fatalf("mapBytesToString = %#v", got)
	}
	if got := mapBytesToString(nil); len(got) != 0 {
		t.Fatalf("mapBytesToString(nil) = %#v, want empty map", got)
	}
}

func TestRegistryRegistersAllConfiguredTopics(t *testing.T) {
	cfg := &config.Config{
		KafkaTopicPrefix: "tinode.",
		BatchSize:        10,
		BatchTimeout:     time.Hour,
		RetryMax:         0,
		RetryBaseDelay:   time.Millisecond,
	}
	r, err := NewRegistry(cfg, nil, nil, logger())
	if err != nil {
		t.Fatalf("NewRegistry returned error: %v", err)
	}
	for _, topic := range cfg.Topics() {
		if _, ok := r.Get(topic); !ok {
			t.Fatalf("missing handler for %s", topic)
		}
	}
	r.StopAll()
}

func TestEventHandlersMapRows(t *testing.T) {
	tests := []struct {
		name  string
		new   func(*[]chclient.Row) Handler
		event proto.Message
		check func(*testing.T, chclient.Row)
	}{
		{
			name: "account",
			new: func(rows *[]chclient.Row) Handler {
				return newAccountHandler("tinode", nil, recordingBatch(rows), logger())
			},
			event: &pbx.AccountEvent{Action: pbx.Crud_CREATE, UserId: "usr1", DefaultAcs: &pbx.DefaultAcsMode{Auth: "JRWPA", Anon: "N"}, Public: []byte(`{"fn":"Alice"}`), Tags: []string{"tag"}},
			check: func(t *testing.T, got chclient.Row) {
				row := got.(accountRow)
				if row.Action != pbx.Crud_CREATE.String() || row.UserID != "usr1" || *row.Public != `{"fn":"Alice"}` || !reflect.DeepEqual(row.Tags, []string{"tag"}) {
					t.Fatalf("account row mismatch: %#v", row)
				}
				if *row.DefaultAcsAuth != "JRWPA" || *row.DefaultAcsAnon != "N" {
					t.Fatalf("default acs mismatch: %#v", row)
				}
			},
		},
		{
			name: "topic",
			new: func(rows *[]chclient.Row) Handler {
				return newTopicHandler("tinode", nil, recordingBatch(rows), logger())
			},
			event: &pbx.TopicEvent{Action: pbx.Crud_UPDATE, Name: "grp1", Desc: &pbx.TopicDesc{CreatedAt: 1, UpdatedAt: 2, TouchedAt: 3, Defacs: &pbx.DefaultAcsMode{Auth: "J", Anon: "N"}, Acs: &pbx.AccessMode{Want: "JRW", Given: "JRW"}, SeqId: 4, Public: []byte("pub"), Private: []byte("priv"), Trusted: []byte("trusted"), State: "ok", StateAt: 5, IsChan: true, Online: true, LastSeenTime: 6, LastSeenUserAgent: "ua"}},
			check: func(t *testing.T, got chclient.Row) {
				row := got.(topicRow)
				if row.Action != pbx.Crud_UPDATE.String() || row.TopicName != "grp1" || *row.DescDefacsAuth != "J" || *row.DescAcsWant != "JRW" || *row.DescPublic != "pub" || !*row.DescIsChan || !*row.DescOnline {
					t.Fatalf("topic row mismatch: %#v", row)
				}
			},
		},
		{
			name: "subscription",
			new: func(rows *[]chclient.Row) Handler {
				return newSubscriptionHandler("tinode", nil, recordingBatch(rows), logger())
			},
			event: &pbx.SubscriptionEvent{Action: pbx.Crud_UPDATE, Topic: "grp1", UserId: "usr1", DelId: 1, ReadId: 2, RecvId: 3, Mode: &pbx.AccessMode{Want: "R", Given: "RW"}, Private: []byte("private")},
			check: func(t *testing.T, got chclient.Row) {
				row := got.(subscriptionRow)
				if row.Action != pbx.Crud_UPDATE.String() || row.Topic != "grp1" || row.UserID != "usr1" || *row.DelID != 1 || *row.ModeWant != "R" || *row.Private != "private" {
					t.Fatalf("subscription row mismatch: %#v", row)
				}
			},
		},
		{
			name: "message",
			new: func(rows *[]chclient.Row) Handler {
				return newMessageHandler("tinode", nil, recordingBatch(rows), logger())
			},
			event: &pbx.MessageEvent{Action: pbx.Crud_CREATE, Msg: &pbx.ServerData{Topic: "grp1", FromUserId: "usr1", Timestamp: 10, DeletedAt: 11, SeqId: 12, Head: map[string][]byte{"mime": []byte("text/plain")}, Content: []byte("hello")}},
			check: func(t *testing.T, got chclient.Row) {
				row := got.(messageRow)
				if row.Action != pbx.Crud_CREATE.String() || row.MsgTopic != "grp1" || *row.MsgFromUser != "usr1" || row.MsgTimestamp != 10 || *row.MsgDeletedAt != 11 || row.MsgSeqID != 12 || row.MsgHead["mime"] != "text/plain" || *row.MsgContent != "hello" {
					t.Fatalf("message row mismatch: %#v", row)
				}
			},
		},
		{
			name: "search",
			new: func(rows *[]chclient.Row) Handler {
				return newSearchHandler("tinode", nil, recordingBatch(rows), logger())
			},
			event: &pbx.SearchQuery{UserId: "usr1", Query: "alice"},
			check: func(t *testing.T, got chclient.Row) {
				row := got.(searchRow)
				if row.UserID != "usr1" || row.Query != "alice" {
					t.Fatalf("search row mismatch: %#v", row)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var rows []chclient.Row
			h := tt.new(&rows)
			if err := h.Handle(msg(marshal(t, tt.event))); err != nil {
				t.Fatalf("Handle returned error: %v", err)
			}
			h.Stop()
			if len(rows) != 1 {
				t.Fatalf("rows = %d, want 1", len(rows))
			}
			tt.check(t, rows[0])
		})
	}
}

func TestHandlersRejectInvalidProtobuf(t *testing.T) {
	handlers := []Handler{
		newAccountHandler("tinode", nil, recordingBatch(&[]chclient.Row{}), logger()),
		newTopicHandler("tinode", nil, recordingBatch(&[]chclient.Row{}), logger()),
		newSubscriptionHandler("tinode", nil, recordingBatch(&[]chclient.Row{}), logger()),
		newMessageHandler("tinode", nil, recordingBatch(&[]chclient.Row{}), logger()),
		newSearchHandler("tinode", nil, recordingBatch(&[]chclient.Row{}), logger()),
		newFirehoseMessagesHandler("tinode", nil, recordingBatch(&[]chclient.Row{}), logger()),
	}
	for _, h := range handlers {
		if err := h.Handle(msg([]byte{0xff})); err == nil {
			t.Fatalf("%T accepted invalid protobuf", h)
		}
		h.Stop()
	}
}

func TestBuildClientReqRowMapsFirehoseVariants(t *testing.T) {
	tests := []struct {
		name  string
		msg   *pbx.ClientMsg
		check func(*testing.T, clientReqRow)
	}{
		{"hi", &pbx.ClientMsg{Message: &pbx.ClientMsg_Hi{Hi: &pbx.ClientHi{Id: "hi1", UserAgent: "ua", Ver: "1", DeviceId: "dev", Lang: "en", Platform: "web", Background: true}}}, func(t *testing.T, row clientReqRow) {
			if row.MsgType != "HI" || row.MsgID != "hi1" || *row.HiUserAgent != "ua" || !*row.HiBackground {
				t.Fatalf("hi row mismatch: %#v", row)
			}
		}},
		{"acc", &pbx.ClientMsg{Message: &pbx.ClientMsg_Acc{Acc: &pbx.ClientAcc{Id: "acc1", UserId: "usr2", Scheme: "basic", Login: true, State: "ok", AuthLevel: pbx.AuthLevel_AUTH, TmpScheme: "reset", Tags: []string{"tag"}}}}, func(t *testing.T, row clientReqRow) {
			if row.MsgType != "ACC" || row.MsgID != "acc1" || *row.AccUserID != "usr2" || !*row.AccLogin || *row.AccAuthLevel != pbx.AuthLevel_AUTH.String() {
				t.Fatalf("acc row mismatch: %#v", row)
			}
		}},
		{"login", &pbx.ClientMsg{Message: &pbx.ClientMsg_Login{Login: &pbx.ClientLogin{Id: "login1", Scheme: "basic"}}}, func(t *testing.T, row clientReqRow) {
			if row.MsgType != "LOGIN" || row.MsgID != "login1" || *row.LoginScheme != "basic" {
				t.Fatalf("login row mismatch: %#v", row)
			}
		}},
		{"sub", &pbx.ClientMsg{Message: &pbx.ClientMsg_Sub{Sub: &pbx.ClientSub{Id: "sub1", Topic: "grp1"}}}, func(t *testing.T, row clientReqRow) {
			if row.MsgType != "SUB" || row.MsgTopic != "grp1" || *row.SubTopic != "grp1" {
				t.Fatalf("sub row mismatch: %#v", row)
			}
		}},
		{"leave", &pbx.ClientMsg{Message: &pbx.ClientMsg_Leave{Leave: &pbx.ClientLeave{Id: "leave1", Topic: "grp1", Unsub: true}}}, func(t *testing.T, row clientReqRow) {
			if row.MsgType != "LEAVE" || row.MsgTopic != "grp1" || !*row.LeaveUnsub {
				t.Fatalf("leave row mismatch: %#v", row)
			}
		}},
		{"pub", &pbx.ClientMsg{Message: &pbx.ClientMsg_Pub{Pub: &pbx.ClientPub{Id: "pub1", Topic: "grp1", NoEcho: true, Head: map[string][]byte{"mime": []byte("text/plain")}, Content: []byte("hello")}}}, func(t *testing.T, row clientReqRow) {
			if row.MsgType != "PUB" || row.MsgTopic != "grp1" || !*row.PubNoEcho || row.PubHead["mime"] != "text/plain" || *row.PubContent != "hello" {
				t.Fatalf("pub row mismatch: %#v", row)
			}
		}},
		{"get", &pbx.ClientMsg{Message: &pbx.ClientMsg_Get{Get: &pbx.ClientGet{Id: "get1", Topic: "grp1", Query: &pbx.GetQuery{What: "sub"}}}}, func(t *testing.T, row clientReqRow) {
			if row.MsgType != "GET" || row.MsgTopic != "grp1" || *row.GetWhat != "sub" {
				t.Fatalf("get row mismatch: %#v", row)
			}
		}},
		{"set", &pbx.ClientMsg{Message: &pbx.ClientMsg_Set{Set: &pbx.ClientSet{Id: "set1", Topic: "grp1"}}}, func(t *testing.T, row clientReqRow) {
			if row.MsgType != "SET" || row.MsgTopic != "grp1" || *row.SetTopic != "grp1" {
				t.Fatalf("set row mismatch: %#v", row)
			}
		}},
		{"del", &pbx.ClientMsg{Message: &pbx.ClientMsg_Del{Del: &pbx.ClientDel{Id: "del1", Topic: "grp1", What: pbx.ClientDel_MSG, UserId: "usr2", Hard: true}}}, func(t *testing.T, row clientReqRow) {
			if row.MsgType != "DEL" || row.MsgTopic != "grp1" || *row.DelWhat != pbx.ClientDel_MSG.String() || *row.DelUserID != "usr2" || !*row.DelHard {
				t.Fatalf("del row mismatch: %#v", row)
			}
		}},
		{"note", &pbx.ClientMsg{Message: &pbx.ClientMsg_Note{Note: &pbx.ClientNote{Topic: "grp1", What: pbx.InfoNote_READ, SeqId: 3, Event: pbx.CallEvent_ACCEPT}}}, func(t *testing.T, row clientReqRow) {
			if row.MsgType != "NOTE" || row.MsgTopic != "grp1" || *row.NoteWhat != pbx.InfoNote_READ.String() || *row.NoteSeqID != 3 || *row.NoteEvent != pbx.CallEvent_ACCEPT.String() {
				t.Fatalf("note row mismatch: %#v", row)
			}
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.msg.Extra = &pbx.ClientExtra{Attachments: []string{"file"}, OnBehalfOf: "root", AuthLevel: pbx.AuthLevel_ROOT}
			row := buildClientReqRow(&pbx.ClientReq{Sess: &pbx.Session{SessionId: "sess1", UserId: "usr1", AuthLevel: pbx.AuthLevel_AUTH, RemoteAddr: "127.0.0.1", UserAgent: "ua", DeviceId: "dev", Language: "en"}, Msg: tt.msg}, time.UnixMilli(1700000000123))
			if row.SessSessionID != "sess1" || row.SessUserID != "usr1" || row.SessAuthLevel != pbx.AuthLevel_AUTH.String() || *row.ExtraOnBehalfOf != "root" || *row.ExtraAuthLevel != pbx.AuthLevel_ROOT.String() {
				t.Fatalf("common row fields mismatch: %#v", row)
			}
			tt.check(t, row)
		})
	}
}
