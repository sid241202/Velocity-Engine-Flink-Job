package in.gov.uidai.dp.velocity.engine.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

/**
 * Root rule object. Deserialised from the new Velocity Engine rule schema.
 *
 * <p>Example:
 * <pre>
 * {
 *   "rule_metadata": { "rule_id": "rule_brute_force_12", "status": "ACTIVE", ... },
 *   "execution_routing": { "target_cluster": "HDC_CLUSTER_1", "target_source_topic": "auth_events_raw" },
 *   "filters": [ { "field": "_data.authResult", "operator": "EQUALS", "value": "y" } ],
 *   "grouping": { "keys": ["_data.enrolmentReferenceId"] },
 *   "windowing": { "type": "SLIDING", "time_type": "EVENT_TIME", "size_ms": 300000, "slide_ms": 60000 },
 *   "aggregations": [ { "alias": "unique_aua", "field": "_data.aua", "function": "COUNT_DISTINCT" } ],
 *   "having_thresholds": { "logic_operator": "AND", "conditions": [ ... ] }
 * }
 * </pre>
 */
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

    private List<FilterCondition> filters;

    private GroupingConfig grouping;

    private WindowingConfig windowing;

    private List<AggregationSpec> aggregations;

    @JsonProperty("having_thresholds")
    private HavingThresholds havingThresholds;

    // ── Convenience delegates ──────────────────────────────────────────────────

    public String getRuleId()        { return ruleMetadata != null ? ruleMetadata.getRuleId()        : null; }
    public String getRuleName()      { return ruleMetadata != null ? ruleMetadata.getRuleName()      : null; }
    public String getStatus()        { return ruleMetadata != null ? ruleMetadata.getStatus()        : null; }
    public String getSeverityLevel() { return ruleMetadata != null ? ruleMetadata.getSeverityLevel() : null; }
    public int    getPenaltyTtlSeconds() { return ruleMetadata != null ? ruleMetadata.getPenaltyTtlSeconds() : 0; }

    public boolean isActive()  { return "ACTIVE".equalsIgnoreCase(getStatus()); }
    public boolean isPaused()  { return "PAUSED".equalsIgnoreCase(getStatus()); }
    public boolean isDeleted() { return "DELETED".equalsIgnoreCase(getStatus()); }

    public String getTargetSourceTopic() { return executionRouting != null ? executionRouting.getTargetSourceTopic() : null; }
    public String getTargetCluster()     { return executionRouting != null ? executionRouting.getTargetCluster()     : null; }
}