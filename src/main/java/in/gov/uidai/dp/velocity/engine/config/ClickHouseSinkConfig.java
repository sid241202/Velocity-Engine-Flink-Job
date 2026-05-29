package in.gov.uidai.dp.velocity.engine.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

@Data @NoArgsConstructor @JsonIgnoreProperties(ignoreUnknown = true)
public class ClickHouseSinkConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    private boolean enabled = true;
    private List<String> hosts;
    private String user = "default";
    private String password;
    @JsonProperty("cluster-name") private String clusterName;
    @JsonProperty("use-distributed") private boolean useDistributed = false;
    @JsonProperty("zookeeper-path") private String zookeeperPath;
    private String database;
    private String table = "rule_results";
    @JsonProperty("auto-create-ddl") private boolean autoCreateDdl = true;
    @JsonProperty("num-writers") private int numWriters = 4;
    @JsonProperty("queue-max-capacity") private int queueMaxCapacity = 10000;
    @JsonProperty("max-buffer-size") private int maxBufferSize = 5000;
    @JsonProperty("flush-interval-ms") private long flushIntervalMs = 5000L;
    @JsonProperty("timeout-sec") private int timeoutSec = 60;
    @JsonProperty("num-retries") private int numRetries = 3;
    @JsonProperty("failed-records-path") private String failedRecordsPath = "/opt/flink/failed_records";

    public String getFirstHost() {
        return (hosts != null && !hosts.isEmpty()) ? hosts.get(0) : null;
    }

    /** Build HTTP URL for the first (or only) ClickHouse host. */
    public String getFirstHostUrl() {
        String h = getFirstHost();
        return h != null ? "http://" + h : null;
    }
}