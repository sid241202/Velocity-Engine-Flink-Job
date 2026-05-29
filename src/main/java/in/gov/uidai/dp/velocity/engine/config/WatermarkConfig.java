package in.gov.uidai.dp.velocity.engine.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data @NoArgsConstructor @JsonIgnoreProperties(ignoreUnknown = true)
public class WatermarkConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    @JsonProperty("idleness-ms") private long idlenessMs = 60_000L;
}