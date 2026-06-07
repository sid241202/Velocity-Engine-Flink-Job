package in.gov.uidai.dp.velocity.engine.config;

public final class AuthDemoConfig {
    public static final String KAFKA_BOOTSTRAP = env("KAFKA_BOOTSTRAP_SERVERS", "broker:29092");
    public static final String AUTH_TOPIC = env("AUTH_EVENTS_TOPIC", "BI.AUTH.AUTH_TXN.UNION.V1");
    public static final String AUTH_CONSUMER_GROUP = env("AUTH_CONSUMER_GROUP", "velocity-engine-auth");
    public static final String RULES_TOPIC = env("RULES_TOPIC", "DE.AUTH.VELOCITY_ENGINE.RULES");
    public static final String RULES_CONSUMER_GROUP = env("RULES_CONSUMER_GROUP", "velocity-engine-rules");
    public static final String RESULTS_TOPIC = env("RESULTS_TOPIC", "DE.AUTH.VELOCITY_ENGINE.RESULTS");
    public static final String ALERTS_TOPIC = env("ALERTS_TOPIC", "DE.AUTH.VELOCITY_ENGINE.ALERTS");
    public static final String CHECKPOINT_DIR = env("CHECKPOINT_DIR", "file:///tmp/flink-checkpoints/velocity-auth");
    public static final String CLUSTER_NAME = env("CLUSTER_NAME", "auth-cluster");
    public static final String CH_HOSTS = env("CLICKHOUSE_HOSTS", "clickhouse:8123");
    public static final String CH_USER = env("CLICKHOUSE_USER", "default");
    public static final String CH_PASSWORD = env("CLICKHOUSE_PASSWORD", "");
    public static final String CH_DATABASE = env("CLICKHOUSE_DATABASE", "auth_analytics");
    public static final String CH_TABLE = env("CLICKHOUSE_TABLE", "velocity_rule_results");
    public static final int CH_BATCH_SIZE = intEnv("CLICKHOUSE_BATCH_SIZE", 5000);
    public static final long CH_FLUSH_INTERVAL_MS = longEnv("CLICKHOUSE_FLUSH_INTERVAL_MS", 5000);
    public static final long MAX_WATERMARK_LAG_MS = longEnv("MAX_WATERMARK_LAG_MS", 60000);
    public static final long DEDUP_TTL_MINUTES = longEnv("DEDUP_TTL_MINUTES", 15);
    public static final String EVENT_TIMESTAMP_FIELD = env("EVENT_TIMESTAMP_FIELD", "_event_timestamp");
    public static final String EVENT_TIMESTAMP_FORMAT = env("EVENT_TIMESTAMP_FORMAT", "ISO_STRING");

    private AuthDemoConfig() {}

    private static String env(String key, String defaultValue) {
        String val = System.getenv(key);
        return (val != null && !val.isEmpty()) ? val : defaultValue;
    }

    private static int intEnv(String key, int defaultValue) {
        String val = System.getenv(key);
        if (val == null || val.isEmpty()) return defaultValue;
        try {
            return Integer.parseInt(val);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private static long longEnv(String key, long defaultValue) {
        String val = System.getenv(key);
        if (val == null || val.isEmpty()) return defaultValue;
        try {
            return Long.parseLong(val);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }
}
