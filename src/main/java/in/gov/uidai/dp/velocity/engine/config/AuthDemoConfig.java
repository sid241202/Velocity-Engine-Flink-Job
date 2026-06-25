package in.gov.uidai.dp.velocity.engine.config;

public final class AuthDemoConfig {

    // ==================== KAFKA ====================
    public static final String KAFKA_BOOTSTRAP = "10.10.107.67:9092,10.10.106.91:9092,10.10.107.133:9092,10.10.107.103:9092,10.10.107.112:9092,10.10.107.177:9092,10.10.107.105:9092";
    public static final String AUTH_TOPIC             = "BI.AUTH.AUTH_TXN.UNION.V1";
    public static final String AUTH_CONSUMER_GROUP    = "STROT.APPLICATION.VELOCITY.ENGINE";
    public static final String RULES_TOPIC            = "DE.AUTH.VELOCITY_ENGINE.RULES";
    public static final String RULES_CONSUMER_GROUP   = "STROT.APPLICATION.VELOCITY.ENGINE";
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

    // ==================== REDIS ====================
    public static final String REDIS_MODE               = "STANDALONE";
    public static final String REDIS_HOSTS              = "127.0.0.1:6379";
    public static final String REDIS_PASSWORD           = "";
    public static final int    REDIS_MAX_TOTAL          = 16;
    public static final int    REDIS_MAX_IDLE           = 8;
    public static final int    REDIS_MIN_IDLE           = 2;
    public static final int    REDIS_TIMEOUT_MS         = 3000;
    public static final int    REDIS_CONNECT_TIMEOUT_MS = 2000;
    public static final int    REDIS_MAX_ATTEMPTS       = 5;
    /** Fallback TTL (seconds) for the Redis penalty key when the rule does not specify one. */
    public static final int    REDIS_DEFAULT_TTL_SECONDS = 3600; // 1 hour

    // ==================== CHECKPOINT ====================
    public static final String CHECKPOINT_DIR = "s3a://prd-bi-data-platform-configs/flink/checkpoints/velocity-auth/";

    // ==================== TUNING ====================
    public static final long MAX_WATERMARK_LAG_MS = 60000;
    public static final long DEDUP_TTL_MINUTES    = 15;

    // ==================== EVENT TIMESTAMP ====================
    public static final String EVENT_TIMESTAMP_FIELD  = "_event_timestamp";
    public static final String EVENT_TIMESTAMP_FORMAT = "ISO_STRING";

    private AuthDemoConfig() {}
}
