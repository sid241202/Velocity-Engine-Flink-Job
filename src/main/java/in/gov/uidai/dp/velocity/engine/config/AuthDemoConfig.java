package in.gov.uidai.dp.velocity.engine.config;

import java.io.Serializable;
import java.util.Properties;

public class AuthDemoConfig implements Serializable {

    // ── LOCAL KAFKA BROKER ─────────────────────
    public static final String KAFKA_BOOTSTRAP_SERVERS = "broker:29092";

    // Auth events topic
    public static final String AUTH_TOPIC = "BI.AUTH.AUTH_TXN.UNION.V1";
    public static final String CONSUMER_GROUP = "velocity-engine-auth-local";

    // Rules topic – UI backend publishes here
    public static final String RULES_TOPIC = "DE.AUTH.VELOCITY_ENGINE.RULES";
    public static final String RULES_CONSUMER_GROUP = "velocity-engine-rules-local";

    // Checkpointing: local filesystem for local testing
    public static final long CHECKPOINT_INTERVAL_MS = 30000L;
    public static final long CHECKPOINT_TIMEOUT_MS  = 120000L;
    public static final long CHECKPOINT_MIN_PAUSE_MS = 10000L;
    public static final String CHECKPOINT_STORAGE = "file:///tmp/flink-checkpoints/velocity-auth";

    // ── ClickHouse: HTTP interface (local single-node) ───────────────────
    public static final String CLICKHOUSE_HOSTS    = "clickhouse:8123";
    public static final String CLICKHOUSE_USER     = "default";
    public static final String CLICKHOUSE_PASSWORD = "password";
    public static final String CLICKHOUSE_DATABASE = "auth_analytics";
    public static final String CLICKHOUSE_TABLE    = "velocity_rule_results";

    public static final long IDLENESS_MS = 30000L;

    // Max watermark lateness at the source level — per-rule lateness is handled in RuleEvaluatorFunction
    public static final long SOURCE_MAX_LATENESS_MS = 60000L;

    public static Properties getClickHouseSinkProperties() {
        Properties props = new Properties();
        props.put("clickhouse.sink.target-table", CLICKHOUSE_DATABASE + "." + CLICKHOUSE_TABLE);
        props.put("clickhouse.sink.max-buffer-size", "5000");
        props.put("clickhouse.sink.flush-interval", "5000"); // 5 sec flush
        return props;
    }
}
