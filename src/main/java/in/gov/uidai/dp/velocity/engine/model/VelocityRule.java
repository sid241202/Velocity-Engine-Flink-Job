package in.gov.uidai.dp.velocity.engine.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class VelocityRule implements Serializable {

    private static final long serialVersionUID = 1L;

    @JsonProperty("rule_metadata")
    private RuleMetadata ruleMetadata;

    @JsonProperty("execution_routing")
    private ExecutionRouting executionRouting;

    private FilterNode filters;
    private GroupingConfig grouping;
    private WindowingConfig windowing;
    private List<AggregationSpec> aggregations;

    @JsonProperty("having_thresholds")
    private HavingThresholds havingThresholds;

    public String getRuleId()        { return ruleMetadata != null ? ruleMetadata.getRuleId()        : null; }
    public String getRuleName()      { return ruleMetadata != null ? ruleMetadata.getRuleName()      : null; }
    public String getStatus()        { return ruleMetadata != null ? ruleMetadata.getStatus()        : null; }
    public String getSeverityLevel() { return ruleMetadata != null ? ruleMetadata.getSeverityLevel() : null; }
    public int    getPenaltyTtlSeconds() { return ruleMetadata != null ? ruleMetadata.getPenaltyTtlSeconds() : 0; }

    public boolean isActive()  { return "ACTIVE".equalsIgnoreCase(getStatus()); }
    public boolean isPaused()  { return "PAUSED".equalsIgnoreCase(getStatus()); }
    public boolean isDeleted() { return "DELETED".equalsIgnoreCase(getStatus()); }

    public String getSourceTopic()   { return executionRouting != null ? executionRouting.getTargetSourceTopic() : null; }
    public String getSourceCluster() { return executionRouting != null ? executionRouting.getTargetCluster()     : null; }
}