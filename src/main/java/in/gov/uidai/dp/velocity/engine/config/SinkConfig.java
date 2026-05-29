package in.gov.uidai.dp.velocity.engine.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data @NoArgsConstructor @JsonIgnoreProperties(ignoreUnknown = true)
public class SinkConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    private KeyDBSinkConfig    keydb      = new KeyDBSinkConfig();
    private ClickHouseSinkConfig clickhouse = new ClickHouseSinkConfig();
    private IcebergSinkConfig  iceberg    = new IcebergSinkConfig();
}