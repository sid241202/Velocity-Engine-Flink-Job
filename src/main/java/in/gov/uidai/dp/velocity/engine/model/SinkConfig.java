package in.gov.uidai.dp.velocity.engine.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class SinkConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    @JsonProperty("agg_sink_enabled")
    private boolean aggSinkEnabled = true;

    @JsonProperty("anomaly_sink_enabled")
    private boolean anomalySinkEnabled = true;

    @JsonProperty("anomaly_store_sink_enabled")
    private boolean anomalyStoreSinkEnabled = true;

    public static SinkConfig allEnabled() {
        return new SinkConfig(true, true, true);
    }
}
