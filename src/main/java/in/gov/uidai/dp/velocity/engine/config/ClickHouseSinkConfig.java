package in.gov.uidai.dp.velocity.engine.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ClickHouseSinkConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    private List<String> hosts;
    private String database;
    @Builder.Default
    private String table = "rule_results";
    @Builder.Default
    private String user = "default";
    private String password;
    @Builder.Default
    private int maxBufferSize = 5000;
    @Builder.Default
    private long flushIntervalMs = 5000L;
    @Builder.Default
    private boolean useDistributed = true;
    private String clusterName;
    private String zookeeperPath;
    private String replicaName;
    @Builder.Default
    private boolean autoCreateDdl = true;

    public String getFirstHost() {
        return (hosts != null && !hosts.isEmpty()) ? hosts.get(0) : null;
    }

    public String getFirstHostUrl() {
        String h = getFirstHost();
        return h != null ? "http://" + h : null;
    }
}