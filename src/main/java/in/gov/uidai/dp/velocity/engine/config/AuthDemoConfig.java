package in.gov.uidai.dp.velocity.engine.config;

import java.io.Serializable;
import java.util.Properties;

public class AuthDemoConfig implements Serializable {

    // ── PROD KAFKA BROKERS ─────────────────────
    public static final String KAFKA_BOOTSTRAP_SERVERS = "10.10.107.67:9092,10.10.106.91:9092,10.10.107.133:9092,10.10.107.103:9092,10.10.107.112:9092,10.10.107.177:9092,10.10.107.105:9092";

    // Auth events topic
    public static final String AUTH_TOPIC = "BI.AUTH.AUTH_TXN.UNION.V1";
    public static final String CONSUMER_GROUP = "STROT.APPLICATION.VELOCITY_ENGINE_AUTH_DEMO";

    // Rules topic – UI backend publishes here
    public static final String RULES_TOPIC = "DE.AUTH.VELOCITY_ENGINE.RULES";
    public static final String RULES_CONSUMER_GROUP = "STROT.APPLICATION.VELOCITY_ENGINE_RULES_DEMO";

    // Checkpointing: S3 for production
    public static final long CHECKPOINT_INTERVAL_MS = 60000L;
    public static final long CHECKPOINT_TIMEOUT_MS  = 600000L;
    public static final long CHECKPOINT_MIN_PAUSE_MS = 15000L;
    public static final String CHECKPOINT_STORAGE = "s3a://prd-bi-data-platform-configs/flink/checkpoints/velocity-auth-demo/";

    // ── ClickHouse: HTTP interface ───────────────────
    public static final String CLICKHOUSE_HOSTS    = "10.10.120.85:8123";
    public static final String CLICKHOUSE_USER     = "default";
    public static final String CLICKHOUSE_PASSWORD = "qwerty";
    public static final String CLICKHOUSE_DATABASE = "auth_analytics";
    public static final String CLICKHOUSE_TABLE    = "velocity_rule_results";

    public static final long IDLENESS_MS = 60000L;

    public static Properties getClickHouseSinkProperties() {
        Properties props = new Properties();
        props.put("clickhouse.sink.target-table", CLICKHOUSE_DATABASE + "." + CLICKHOUSE_TABLE);
        props.put("clickhouse.sink.max-buffer-size", "5000"); 
        props.put("clickhouse.sink.flush-interval", "5000"); // 5 sec flush
        return props;
    }
}
