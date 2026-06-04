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
public class ExecutionRouting implements Serializable {

    private static final long serialVersionUID = 1L;

    @JsonProperty("target_cluster")
    private String targetCluster;

    @JsonProperty("target_source_topic")
    private String targetSourceTopic;
}