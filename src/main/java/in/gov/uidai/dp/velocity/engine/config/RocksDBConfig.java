package in.gov.uidai.dp.velocity.engine.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data @NoArgsConstructor @JsonIgnoreProperties(ignoreUnknown = true)
public class RocksDBConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    private boolean incremental = true;
    @JsonProperty("memory-per-slot-mb")    private int    memoryPerSlotMb    = 256;
    @JsonProperty("write-buffer-ratio")    private double writeBufferRatio   = 0.5;
    @JsonProperty("high-prio-pool-ratio")  private double highPrioPoolRatio  = 0.1;
}