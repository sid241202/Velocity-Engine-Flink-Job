package in.gov.uidai.dp.velocity.engine.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class RuleSnapshot implements Serializable {
    private static final long serialVersionUID = 2L;

    private String ruleId;
    private String entityName;
    private String anomalyEntityField;
    private int penaltyTtlSeconds;
    private String sourceTopic;
    private String cluster;
    private SinkConfig sinks;
    private WindowingConfig windowing;
    private List<AggregationSpec> aggregations;
    private HavingThresholds havingThresholds;
    private long allowedLatenessMs;

    public static RuleSnapshot fromRule(VelocityRule rule, String cluster) {
        return new RuleSnapshot(
                rule.getRuleId(),
                rule.getEntityName(),
                rule.getAnomalyEntityField(),
                rule.getPenaltyTtlSeconds(),
                rule.getSourceTopic(),
                cluster,
                rule.getEffectiveSinks(),
                rule.getWindowing(),
                rule.getAggregations(),
                rule.getHavingThresholds(),
                rule.getWindowing() != null ? rule.getWindowing().getAllowedLatenessMs() : 0L
        );
    }
}