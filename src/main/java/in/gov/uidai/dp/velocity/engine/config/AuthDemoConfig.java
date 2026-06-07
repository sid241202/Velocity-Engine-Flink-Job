package in.gov.uidai.dp.velocity.engine.config;

public final class AuthDemoConfig {

    // ==================== KAFKA ====================
    public static final String KAFKA_BOOTSTRAP = "broker:29092";
    public static final String AUTH_TOPIC = "BI.AUTH.AUTH_TXN.UNION.V1";
    public static final String AUTH_CONSUMER_GROUP = "velocity-engine-auth";
    public static final String RULES_TOPIC = "DE.AUTH.VELOCITY_ENGINE.RULES";
    public static final String RULES_CONSUMER_GROUP = "velocity-engine-rules";
    public static final String RESULTS_TOPIC = "DE.AUTH.VELOCITY_ENGINE.RESULTS";
    public static final String ALERTS_TOPIC = "DE.AUTH.VELOCITY_ENGINE.ALERTS";

    // ==================== CLICKHOUSE ====================
    public static final String CH_HOSTS = "clickhouse:8123";
    public static final String CH_USER = "default";
    public static final String CH_PASSWORD = "";
    public static final String CH_DATABASE = "auth_analytics";
    public static final String CH_TABLE = "velocity_rule_results";
    public static final int CH_BATCH_SIZE = 5000;
    public static final long CH_FLUSH_INTERVAL_MS = 5000;

    // ==================== CHECKPOINT ====================
    public static final String CHECKPOINT_DIR = "file:///tmp/flink-checkpoints/velocity-auth";

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
