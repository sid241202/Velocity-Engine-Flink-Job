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

    private static final long serialVersionUID = 1L;

    private String ruleId;
    private String ruleName;
    private String severityLevel;
    private int    penaltyTtlSeconds;
    private String sourceTopic;
    private String cluster;
    private WindowingConfig     windowing;
    private List<AggregationSpec> aggregations;
    private HavingThresholds    havingThresholds;

    public static RuleSnapshot fromRule(VelocityRule rule, String cluster) {
        return new RuleSnapshot(
                rule.getRuleId(),
                rule.getRuleName(),
                rule.getSeverityLevel(),
                rule.getPenaltyTtlSeconds(),
                rule.getTargetSourceTopic(),
                cluster,
                rule.getWindowing(),
                rule.getAggregations(),
                rule.getHavingThresholds()
        );
    }
}