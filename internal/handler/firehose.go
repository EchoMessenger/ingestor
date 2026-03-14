package handler

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	chclient "github.com/EchoMessenger/ingestor/internal/clickhouse"
	pbx "github.com/EchoMessenger/ingestor/pbx"

	"github.com/IBM/sarama"
	"google.golang.org/protobuf/proto"
)

// clientReqRow соответствует схеме client_req_log.
// Все firehose топики пишут в одну таблицу —
// незаполненные поля остаются nil (Nullable колонки).
type clientReqRow struct {
	LogID        string
	LogTimestamp time.Time

	// Session
	SessSessionID  string
	SessUserID     string
	SessAuthLevel  string
	SessRemoteAddr string
	SessUserAgent  string
	SessDeviceID   string
	SessLanguage   string

	// Message type
	MsgType  string
	MsgID    string
	MsgTopic string

	// Extra
	ExtraAttachments  []string
	ExtraOnBehalfOf   *string
	ExtraAuthLevel    *string

	// ClientHi
	HiUserAgent *string
	HiVer       *string
	HiDeviceID  *string
	HiLang      *string
	HiPlatform  *string
	HiBackground *bool

	// ClientAcc
	AccUserID     *string
	AccScheme     *string
	AccLogin      *bool
	AccState      *string
	AccAuthLevel  *string
	AccTmpScheme  *string
	AccTags       []string

	// ClientLogin
	LoginScheme *string

	// ClientSub
	SubTopic *string

	// ClientLeave
	LeaveUnsub *bool

	// ClientPub
	PubNoEcho  *bool
	PubHead    map[string]string
	PubContent *string

	// ClientGet
	GetWhat *string

	// ClientSet
	SetTopic *string

	// ClientDel
	DelWhat   *string
	DelUserID *string
	DelHard   *bool

	// ClientNote
	NoteWhat  *string
	NoteSeqID *int32
	NoteEvent *string
}

// ---- flush ----

func flushClientReqRows(ctx context.Context, ch *chclient.Client, rows []chclient.Row) error {
	batch, err := ch.Conn().PrepareBatch(ctx, `
		INSERT INTO client_req_log (
			log_id, log_timestamp,
			sess_session_id, sess_user_id, sess_auth_level,
			sess_remote_addr, sess_user_agent, sess_device_id, sess_language,
			msg_type, msg_id, msg_topic,
			extra_attachments, extra_on_behalf_of, extra_auth_level,
			hi_user_agent, hi_ver, hi_device_id, hi_lang, hi_platform, hi_background,
			acc_user_id, acc_scheme, acc_login, acc_state, acc_auth_level, acc_tmp_scheme, acc_tags,
			login_scheme,
			sub_topic,
			leave_unsub,
			pub_no_echo, pub_head, pub_content,
			get_what,
			set_topic,
			del_what, del_user_id, del_hard,
			note_what, note_seq_id, note_event
		)`)
	if err != nil {
		return fmt.Errorf("client_req_log: prepare: %w", err)
	}

	for _, r := range rows {
		row := r.(clientReqRow)
		if err := batch.Append(
			row.LogID, row.LogTimestamp,
			row.SessSessionID, row.SessUserID, row.SessAuthLevel,
			row.SessRemoteAddr, row.SessUserAgent, row.SessDeviceID, row.SessLanguage,
			row.MsgType, row.MsgID, row.MsgTopic,
			row.ExtraAttachments, row.ExtraOnBehalfOf, row.ExtraAuthLevel,
			row.HiUserAgent, row.HiVer, row.HiDeviceID, row.HiLang, row.HiPlatform, row.HiBackground,
			row.AccUserID, row.AccScheme, row.AccLogin, row.AccState, row.AccAuthLevel, row.AccTmpScheme, row.AccTags,
			row.LoginScheme,
			row.SubTopic,
			row.LeaveUnsub,
			row.PubNoEcho, row.PubHead, row.PubContent,
			row.GetWhat,
			row.SetTopic,
			row.DelWhat, row.DelUserID, row.DelHard,
			row.NoteWhat, row.NoteSeqID, row.NoteEvent,
		); err != nil {
			return fmt.Errorf("client_req_log: append: %w", err)
		}
	}

	return batch.Send()
}

// ---- base firehose handler ----
// Все firehose топики пишут в одну таблицу и один batch.
// Каждый handler — отдельный struct но shared batch через замыкание.

type firehoseHandler struct {
	topicName string
	msgType   string
	batch     *chclient.Batch
	log       *slog.Logger
}

func newFirehoseHandler(topicSuffix, msgType, prefix string, ch *chclient.Client, mkBatch makeBatchFn, log *slog.Logger) *firehoseHandler {
	h := &firehoseHandler{
		topicName: topic(prefix, topicSuffix),
		msgType:   msgType,
		log:       log,
	}
	// Каждый firehose handler имеет свой batch → своя горутина flush.
	// Все пишут в одну таблицу client_req_log.
	h.batch = mkBatch("client_req_log/"+msgType, func(ctx context.Context, rows []chclient.Row) error {
		return flushClientReqRows(ctx, ch, rows)
	})
	return h
}

func (h *firehoseHandler) Topic() string { return h.topicName }
func (h *firehoseHandler) Stop()         { h.batch.Stop() }

func (h *firehoseHandler) handle(msg *sarama.ConsumerMessage) error {
	var req pbx.ClientReq
	if err := proto.Unmarshal(msg.Value, &req); err != nil {
		return fmt.Errorf("firehose[%s]: unmarshal: %w", h.msgType, err)
	}

	row := buildClientReqRow(&req, msg.Timestamp)

	h.batch.Add(row)

	h.log.Debug("handler.firehose.add",
		"msg_type", h.msgType,
		"user_id", row.SessUserID,
		"topic", row.MsgTopic,
	)
	return nil
}

// buildClientReqRow — единая функция маппинга ClientReq → clientReqRow.
func buildClientReqRow(req *pbx.ClientReq, ts time.Time) clientReqRow {
	sess := req.GetSess()
	m := req.GetMsg()
	extra := m.GetExtra()

	row := clientReqRow{
		LogID:        newUUID(),
		LogTimestamp: ts.UTC(),

		SessSessionID:  sess.GetSessionId(),
		SessUserID:     sess.GetUserId(),
		SessAuthLevel:  sess.GetAuthLevel().String(),
		SessRemoteAddr: sess.GetRemoteAddr(),
		SessUserAgent:  sess.GetUserAgent(),
		SessDeviceID:   sess.GetDeviceId(),
		SessLanguage:   sess.GetLanguage(),

		ExtraAttachments: extra.GetAttachments(),
		ExtraOnBehalfOf:  nullableString(extra.GetOnBehalfOf()),
	}

	if lvl := extra.GetAuthLevel(); lvl != pbx.AuthLevel_NONE {
		s := lvl.String()
		row.ExtraAuthLevel = &s
	}

	switch {
	case m.GetHi() != nil:
		hi := m.GetHi()
		row.MsgType = "HI"
		row.MsgID = hi.GetId()
		row.HiUserAgent = nullableString(hi.GetUserAgent())
		row.HiVer = nullableString(hi.GetVer())
		row.HiDeviceID = nullableString(hi.GetDeviceId())
		row.HiLang = nullableString(hi.GetLang())
		row.HiPlatform = nullableString(hi.GetPlatform())
		bg := hi.GetBackground()
		row.HiBackground = &bg

	case m.GetAcc() != nil:
		acc := m.GetAcc()
		row.MsgType = "ACC"
		row.MsgID = acc.GetId()
		row.AccUserID = nullableString(acc.GetUserId())
		row.AccScheme = nullableString(acc.GetScheme())
		login := acc.GetLogin()
		row.AccLogin = &login
		row.AccState = nullableString(acc.GetState())
		lvl := acc.GetAuthLevel().String()
		row.AccAuthLevel = &lvl
		row.AccTmpScheme = nullableString(acc.GetTmpScheme())
		row.AccTags = acc.GetTags()

	case m.GetLogin() != nil:
		login := m.GetLogin()
		row.MsgType = "LOGIN"
		row.MsgID = login.GetId()
		row.LoginScheme = nullableString(login.GetScheme())
		// Secret намеренно не пишем — зачищается в router'е

	case m.GetSub() != nil:
		sub := m.GetSub()
		row.MsgType = "SUB"
		row.MsgID = sub.GetId()
		row.MsgTopic = sub.GetTopic()
		row.SubTopic = nullableString(sub.GetTopic())

	case m.GetLeave() != nil:
		leave := m.GetLeave()
		row.MsgType = "LEAVE"
		row.MsgID = leave.GetId()
		row.MsgTopic = leave.GetTopic()
		unsub := leave.GetUnsub()
		row.LeaveUnsub = &unsub

	case m.GetPub() != nil:
		pub := m.GetPub()
		row.MsgType = "PUB"
		row.MsgID = pub.GetId()
		row.MsgTopic = pub.GetTopic()
		noEcho := pub.GetNoEcho()
		row.PubNoEcho = &noEcho
		row.PubHead = mapBytesToString(pub.GetHead())
		row.PubContent = nullableString(string(pub.GetContent()))

	case m.GetGet() != nil:
		get := m.GetGet()
		row.MsgType = "GET"
		row.MsgID = get.GetId()
		row.MsgTopic = get.GetTopic()
		row.GetWhat = nullableString(get.GetQuery().GetWhat())

	case m.GetSet() != nil:
		set := m.GetSet()
		row.MsgType = "SET"
		row.MsgID = set.GetId()
		row.MsgTopic = set.GetTopic()
		row.SetTopic = nullableString(set.GetTopic())

	case m.GetDel() != nil:
		del := m.GetDel()
		row.MsgType = "DEL"
		row.MsgID = del.GetId()
		row.MsgTopic = del.GetTopic()
		what := del.GetWhat().String()
		row.DelWhat = &what
		row.DelUserID = nullableString(del.GetUserId())
		hard := del.GetHard()
		row.DelHard = &hard

	case m.GetNote() != nil:
		note := m.GetNote()
		row.MsgType = "NOTE"
		row.MsgTopic = note.GetTopic()
		what := note.GetWhat().String()
		row.NoteWhat = &what
		seqID := note.GetSeqId()
		row.NoteSeqID = &seqID
		ev := note.GetEvent().String()
		row.NoteEvent = &ev
	}

	return row
}

// ---- конкретные handlers для каждого топика ----

func newFirehoseHandshakeHandler(prefix string, ch *chclient.Client, mkBatch makeBatchFn, log *slog.Logger) Handler {
	h := newFirehoseHandler("firehose.handshake", "hi", prefix, ch, mkBatch, log)
	return &simpleFirehose{firehoseHandler: h}
}

func newFirehoseAuthHandler(prefix string, ch *chclient.Client, mkBatch makeBatchFn, log *slog.Logger) Handler {
	h := newFirehoseHandler("firehose.auth", "login", prefix, ch, mkBatch, log)
	return &simpleFirehose{firehoseHandler: h}
}

func newFirehoseAccountMgmtHandler(prefix string, ch *chclient.Client, mkBatch makeBatchFn, log *slog.Logger) Handler {
	h := newFirehoseHandler("firehose.account-mgmt", "acc", prefix, ch, mkBatch, log)
	return &simpleFirehose{firehoseHandler: h}
}

func newFirehoseSubscriptionsHandler(prefix string, ch *chclient.Client, mkBatch makeBatchFn, log *slog.Logger) Handler {
	h := newFirehoseHandler("firehose.subscriptions", "sub/leave", prefix, ch, mkBatch, log)
	return &simpleFirehose{firehoseHandler: h}
}

func newFirehoseMessagesHandler(prefix string, ch *chclient.Client, mkBatch makeBatchFn, log *slog.Logger) Handler {
	h := newFirehoseHandler("firehose.messages", "pub", prefix, ch, mkBatch, log)
	return &simpleFirehose{firehoseHandler: h}
}

func newFirehoseQueriesHandler(prefix string, ch *chclient.Client, mkBatch makeBatchFn, log *slog.Logger) Handler {
	h := newFirehoseHandler("firehose.queries", "get", prefix, ch, mkBatch, log)
	return &simpleFirehose{firehoseHandler: h}
}

func newFirehoseUpdatesHandler(prefix string, ch *chclient.Client, mkBatch makeBatchFn, log *slog.Logger) Handler {
	h := newFirehoseHandler("firehose.updates", "set", prefix, ch, mkBatch, log)
	return &simpleFirehose{firehoseHandler: h}
}

func newFirehoseDeletionsHandler(prefix string, ch *chclient.Client, mkBatch makeBatchFn, log *slog.Logger) Handler {
	h := newFirehoseHandler("firehose.deletions", "del", prefix, ch, mkBatch, log)
	return &simpleFirehose{firehoseHandler: h}
}

func newFirehoseNotificationsHandler(prefix string, ch *chclient.Client, mkBatch makeBatchFn, log *slog.Logger) Handler {
	h := newFirehoseHandler("firehose.notifications", "note", prefix, ch, mkBatch, log)
	return &simpleFirehose{firehoseHandler: h}
}

// simpleFirehose оборачивает firehoseHandler реализуя Handler интерфейс.
type simpleFirehose struct {
	*firehoseHandler
}

func (s *simpleFirehose) Handle(msg *sarama.ConsumerMessage) error {
	return s.firehoseHandler.handle(msg)
}