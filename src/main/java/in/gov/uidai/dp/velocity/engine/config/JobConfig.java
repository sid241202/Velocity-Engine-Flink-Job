package in.gov.uidai.dp.velocity.engine.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

/**
 * Top-level configuration POJO. Deserialised from {@code /opt/flink/usrlib/jobconf.yaml}
 * at job startup. One config file per Flink job deployment.
 */
@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class JobConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    @JsonProperty("job-name")    private String jobName;
    @JsonProperty("job-version") private String jobVersion;
    @JsonProperty("job-profile") private String jobProfile;
    private String cluster;
    private String dc;
    @JsonProperty("created-by")  private String createdBy;
    private int parallelism = 1;

    private CheckpointConfig    checkpoint  = new CheckpointConfig();
    private RocksDBConfig       rocksdb     = new RocksDBConfig();
    private WatermarkConfig     watermark   = new WatermarkConfig();
    private KafkaConfig         kafka;
    private List<SourceTopicConfig> sources;
    private PostgresConfig      postgres;
    private SinkConfig          sinks       = new SinkConfig();
}