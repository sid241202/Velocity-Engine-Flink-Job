package in.gov.uidai.dp.velocity.engine.config;

public final class AuthDemoConfig {

    // ==================== KAFKA ====================
    public static final String KAFKA_BOOTSTRAP = "10.10.73.105:9092,10.10.74.200:9092,10.10.74.86:9092";
    public static final String AUTH_TOPIC             = "BI.AUTH.AUTH_TXN.UNION.V1";
    public static final String AUTH_CONSUMER_GROUP    = "STROT.APPLICATION.VELOCITY_ENGINE";
    public static final String RULES_TOPIC            = "DE.AUTH.VELOCITY_ENGINE.RULES";
    public static final String RULES_CONSUMER_GROUP   = "STROT.APPLICATION.VELOCITY_ENGINE";
    public static final String RESULTS_TOPIC          = "DE.AUTH.VELOCITY_ENGINE.RESULTS";
    public static final String ANOMALY_TOPIC          = "DE.AUTH.VELOCITY_ENGINE.ANOMALIES";

    // ==================== CLICKHOUSE ====================
    public static final String CH_HOSTS            = "10.10.120.85:8123";
    public static final String CH_USER             = "default";
    public static final String CH_PASSWORD         = "qwerty";
    public static final String CH_DATABASE         = "auth_analytics";
    public static final String CH_AGG_TABLE        = "auth_velocity_agg_results";
    public static final String CH_ANOMALY_TABLE    = "auth_velocity_anomaly_events";
    public static final int    CH_BATCH_SIZE       = 5000;
    public static final long   CH_FLUSH_INTERVAL_MS = 5000;

    // ==================== KEYDB ====================
    // Redis is being replaced by KeyDB here. KeyDB is RESP-protocol-compatible
    // with Redis, so the Jedis client and every command this repo issues
    // (SET/SETEX only) work unchanged — no client-library or command-level
    // migration needed.
    //
    // ASSUMED_KEYDB_VERSION below is a placeholder target, not a confirmed
    // fact: the actual KeyDB version hasn't been confirmed yet. Nothing in
    // this codebase branches on it today (only baseline SET/SETEX are used,
    // supported by every KeyDB release), so it's purely documentation for
    // now — bump the string once the real version is known.
    public static final String ASSUMED_KEYDB_VERSION     = "6.3.x (unconfirmed placeholder)";
    public static final String KEYDB_MODE               = "STANDALONE";
    // Placeholder — replace with the real KeyDB host:port once known.
    public static final String KEYDB_HOSTS              = "<KEYDB_HOST>:<KEYDB_PORT>";
    // No AUTH was provisioned on the new instance. Empty string (not null)
    // is deliberate: KeyDbConfig/KeyDbSink treat "" the same as unset and
    // skip sending AUTH entirely, so this doesn't need to change unless a
    // password is actually added later — just set it here when it is.
    public static final String KEYDB_PASSWORD           = "";
    public static final int    KEYDB_MAX_TOTAL          = 16;
    public static final int    KEYDB_MAX_IDLE           = 8;
    public static final int    KEYDB_MIN_IDLE           = 2;
    public static final int    KEYDB_TIMEOUT_MS         = 3000;
    public static final int    KEYDB_CONNECT_TIMEOUT_MS = 2000;
    public static final int    KEYDB_MAX_ATTEMPTS       = 5;
    /** Fallback TTL (seconds) for the KeyDB penalty key when the rule does not specify one. */
    public static final int    KEYDB_DEFAULT_TTL_SECONDS = 3600; // 1 hour

    // ==================== CHECKPOINT ====================
    public static final String CHECKPOINT_DIR = "s3a://prd-bi-data-platform-configs/flink/checkpoints/velocity-auth/";

    // ==================== TUNING ====================
    // Job-wide default parallelism for the main event path (Kafka source through
    // rule evaluation). Must not exceed the source topic's partition count (the
    // Kafka source operator can never use more parallel subtasks than there are
    // partitions to assign them) — see PRODUCTION_CAPACITY_SPECS.txt at the repo
    // root for the sizing rationale. Overridable per-environment via
    // VELOCITY_ENGINE_PARALLELISM without a rebuild; defaults to 8, the low end
    // of this topic's known 8-9 partition range, so a stale/unset value never
    // silently exceeds the partition count.
    public static final int JOB_PARALLELISM = Integer.parseInt(
            System.getenv().getOrDefault("VELOCITY_ENGINE_PARALLELISM", "8"));
    public static final long MAX_WATERMARK_LAG_MS = 60000;
    public static final long DEDUP_TTL_MINUTES    = 15;
    /** Minimum wall-clock gap between early-fire (partial) AggregationResult emissions per group key. */
    public static final long EARLY_FIRE_INTERVAL_MS = 3000;

    // ==================== EVENT TIMESTAMP ====================
    public static final String EVENT_TIMESTAMP_FIELD  = "_event_timestamp";
    public static final String EVENT_TIMESTAMP_FORMAT = "ISO_STRING";

    private AuthDemoConfig() {}
}
