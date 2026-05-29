package in.gov.uidai.dp.velocity.engine.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data @NoArgsConstructor @JsonIgnoreProperties(ignoreUnknown = true)
public class CheckpointConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    @JsonProperty("interval-ms") private long intervalMs = 60_000L;
    @JsonProperty("timeout-ms") private long timeoutMs = 600_000L;
    @JsonProperty("min-pause-ms") private long minPauseMs = 15_000L;
    private String mode = "EXACTLY_ONCE";
    private String storage;
    @JsonProperty("retain-on-cancel") private boolean retainOnCancel = true;
    @JsonProperty("unaligned-enabled") private boolean unalignedEnabled = true;
}