package in.gov.uidai.dp.velocity.engine.config;

public final class AuthDemoConfig {

    // ==================== KAFKA ====================
    public static final String KAFKA_BOOTSTRAP = "10.10.107.67:9092,10.10.106.91:9092,10.10.107.133:9092,10.10.107.103:9092,10.10.107.112:9092,10.10.107.177:9092,10.10.107.105:9092";
    public static final String AUTH_TOPIC = "BI.AUTH.AUTH_TXN.UNION.V1";
    public static final String AUTH_CONSUMER_GROUP = "STROT.APPLICATION.VELOCITY_ENGINE";
    public static final String RULES_TOPIC = "DE.AUTH.VELOCITY_ENGINE.RULES";
    public static final String RULES_CONSUMER_GROUP = "STROT.APPLICATION.VELOCITY_ENGINE";
    public static final String RESULTS_TOPIC = "DE.AUTH.VELOCITY_ENGINE.RESULTS";
    public static final String ALERTS_TOPIC = "DE.AUTH.VELOCITY_ENGINE.ALERTS";

    // ==================== CLICKHOUSE ====================
    public static final String CH_HOSTS = "10.10.120.85:8123";
    public static final String CH_USER = "default";
    public static final String CH_PASSWORD = "qwerty";
    public static final String CH_DATABASE = "auth_analytics";
    public static final String CH_TABLE = "auth_velocity_rule_results";
    public static final int CH_BATCH_SIZE = 5000;
    public static final long CH_FLUSH_INTERVAL_MS = 5000;

    // ==================== CHECKPOINT ====================
    public static final String CHECKPOINT_DIR = "s3a://prd-bi-data-platform-configs/flink/checkpoints/velocity-auth/";

    // ==================== CLUSTER ====================
    public static final String CLUSTER_NAME = "auth-cluster";

    // ==================== TUNING ====================
    public static final long MAX_WATERMARK_LAG_MS = 60000;
    public static final long DEDUP_TTL_MINUTES = 15;

    // ==================== EVENT TIMESTAMP ====================
    public static final String EVENT_TIMESTAMP_FIELD = "_event_timestamp";
    public static final String EVENT_TIMESTAMP_FORMAT = "ISO_STRING";

    private AuthDemoConfig() {}
}
