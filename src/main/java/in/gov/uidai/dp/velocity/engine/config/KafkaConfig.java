package in.gov.uidai.dp.velocity.engine.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data @NoArgsConstructor @JsonIgnoreProperties(ignoreUnknown = true)
public class KafkaConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    private String brokers;
    @JsonProperty("rules-topic")    private String rulesTopic;
    @JsonProperty("rules-group-id") private String rulesGroupId;
    @JsonProperty("late-data-topic")private String lateDataTopic;
}