-- account_log
CREATE TABLE IF NOT EXISTS account_log (
    log_id        UUID          DEFAULT generateUUIDv4(),
    log_timestamp DateTime64(3) DEFAULT now64(3),
    action        Enum8('CREATE' = 0, 'UPDATE' = 1, 'DELETE' = 2),
    user_id       String,
    default_acs_auth Nullable(String),
    default_acs_anon Nullable(String),
    public        Nullable(String),
    tags          Array(String)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(log_timestamp)
ORDER BY (log_timestamp, user_id, action)
TTL toDateTime(log_timestamp) + INTERVAL 365 DAY;

-- topic_log
CREATE TABLE IF NOT EXISTS topic_log (
    log_id        UUID          DEFAULT generateUUIDv4(),
    log_timestamp DateTime64(3) DEFAULT now64(3),
    action        Enum8('CREATE' = 0, 'UPDATE' = 1, 'DELETE' = 2),
    topic_name    String,
    desc_created_at        Nullable(Int64),
    desc_updated_at        Nullable(Int64),
    desc_touched_at        Nullable(Int64),
    desc_defacs_auth       Nullable(String),
    desc_defacs_anon       Nullable(String),
    desc_acs_want          Nullable(String),
    desc_acs_given         Nullable(String),
    desc_seq_id            Nullable(Int32),
    desc_read_id           Nullable(Int32),
    desc_recv_id           Nullable(Int32),
    desc_del_id            Nullable(Int32),
    desc_public            Nullable(String),
    desc_private           Nullable(String),
    desc_trusted           Nullable(String),
    desc_state             Nullable(String),
    desc_state_at          Nullable(Int64),
    desc_is_chan           Nullable(Bool),
    desc_online            Nullable(Bool),
    desc_last_seen_time    Nullable(Int64),
    desc_last_seen_user_agent Nullable(String)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(log_timestamp)
ORDER BY (log_timestamp, topic_name, action);

-- subscription_log
CREATE TABLE IF NOT EXISTS subscription_log (
    log_id        UUID          DEFAULT generateUUIDv4(),
    log_timestamp DateTime64(3) DEFAULT now64(3),
    action        Enum8('CREATE' = 0, 'UPDATE' = 1, 'DELETE' = 2),
    topic         String,
    user_id       String,
    del_id        Nullable(Int32),
    read_id       Nullable(Int32),
    recv_id       Nullable(Int32),
    mode_want     Nullable(String),
    mode_given    Nullable(String),
    private       Nullable(String)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(log_timestamp)
ORDER BY (log_timestamp, topic, user_id, action);

-- message_log
CREATE TABLE IF NOT EXISTS message_log (
    log_id        UUID          DEFAULT generateUUIDv4(),
    log_timestamp DateTime64(3) DEFAULT now64(3),
    action        Enum8('CREATE' = 0, 'UPDATE' = 1, 'DELETE' = 2),
    msg_topic     String,
    msg_from_user_id Nullable(String),
    msg_timestamp    Int64,
    msg_deleted_at   Nullable(Int64),
    msg_seq_id       Int32,
    msg_head         Map(String, String),
    msg_content      Nullable(String)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(log_timestamp)
ORDER BY (log_timestamp, msg_topic, msg_seq_id);

-- client_req_log
CREATE TABLE IF NOT EXISTS client_req_log (
    log_id        UUID          DEFAULT generateUUIDv4(),
    log_timestamp DateTime64(3) DEFAULT now64(3),
    sess_session_id  String,
    sess_user_id     String,
    sess_auth_level  String,
    sess_remote_addr String,
    sess_user_agent  String,
    sess_device_id   String,
    sess_language    String,
    msg_type         String,
    msg_id           String,
    msg_topic        String,
    extra_attachments  Array(String),
    extra_on_behalf_of Nullable(String),
    extra_auth_level   Nullable(String),
    hi_user_agent Nullable(String),
    hi_ver        Nullable(String),
    hi_device_id  Nullable(String),
    hi_lang       Nullable(String),
    hi_platform   Nullable(String),
    hi_background Nullable(Bool),
    acc_user_id    Nullable(String),
    acc_scheme     Nullable(String),
    acc_login      Nullable(Bool),
    acc_state      Nullable(String),
    acc_auth_level Nullable(String),
    acc_tmp_scheme Nullable(String),
    acc_tags       Array(String),
    login_scheme   Nullable(String),
    sub_topic      Nullable(String),
    leave_unsub    Nullable(Bool),
    pub_no_echo    Nullable(Bool),
    pub_head       Map(String, String),
    pub_content    Nullable(String),
    get_what       Nullable(String),
    set_topic      Nullable(String),
    del_what       Nullable(String),
    del_user_id    Nullable(String),
    del_hard       Nullable(Bool),
    note_what      Nullable(String),
    note_seq_id    Nullable(Int32),
    note_event     Nullable(String)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(log_timestamp)
ORDER BY (log_timestamp, sess_user_id, msg_type)
TTL toDateTime(log_timestamp) + INTERVAL 90 DAY;

-- search_log (используется только при find_handler: true в tinode.conf)
-- см. internal/handler/search.go
CREATE TABLE IF NOT EXISTS search_log (
    log_id        UUID          DEFAULT generateUUIDv4(),
    log_timestamp DateTime64(3) DEFAULT now64(3),
    user_id       String,
    query         String
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(log_timestamp)
ORDER BY (log_timestamp, user_id)
TTL toDateTime(log_timestamp) + INTERVAL 90 DAY;