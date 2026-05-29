package in.gov.uidai.dp.velocity.engine.config;

import java.io.Serializable;
import java.util.Properties;

public class AuthDemoConfig implements Serializable {
    
    // Kafka Source Constants
    public static final String KAFKA_BOOTSTRAP_SERVERS = "10.10.107.67:9092,10.10.106.91:9092,10.10.107.133:9092,10.10.107.103:9092,10.10.107.112:9092,10.10.107.177:9092,10.10.107.105:9092";
    public static final String AUTH_TOPIC = "BI.AUTH.AUTH_TXN.UNION.V1";
    public static final String CONSUMER_GROUP = "STROT.APPLICATION.VELOCITY_ENGINE_AUTH_DEMO";
    
    // Kafka Rules Source Constants
    public static final String RULES_TOPIC = "uid-dp-velocity-rules-auth-demo";
    public static final String RULES_CONSUMER_GROUP = "STROT.APPLICATION.VELOCITY_ENGINE_RULES_DEMO";
    
    // Checkpointing Constants
    public static final long CHECKPOINT_INTERVAL_MS = 60000L;
    public static final long CHECKPOINT_TIMEOUT_MS = 600000L;
    public static final long CHECKPOINT_MIN_PAUSE_MS = 15000L;
    public static final String CHECKPOINT_STORAGE = "s3a://prd-bi-data-platform-configs/flink/checkpoints/velocity-auth-demo/";
    
    // ClickHouse Sink Constants
    public static final String CLICKHOUSE_HOSTS = "10.10.120.85:8123";
    public static final String CLICKHOUSE_USER = "default";
    public static final String CLICKHOUSE_PASSWORD = "qwerty"; // Hardcoded as requested
    public static final String CLICKHOUSE_DATABASE = "auth_analytics";
    public static final String CLICKHOUSE_TABLE = "velocity_rule_results";
    
    // Watermark Constants
    public static final long IDLENESS_MS = 60000L;

    public static Properties getClickHouseSinkProperties() {
        Properties props = new Properties();
        props.put("clickhouse.sink.target-table", CLICKHOUSE_DATABASE + "." + CLICKHOUSE_TABLE);
        props.put("clickhouse.sink.max-buffer-size", "5000");
        return props;
    }
}
