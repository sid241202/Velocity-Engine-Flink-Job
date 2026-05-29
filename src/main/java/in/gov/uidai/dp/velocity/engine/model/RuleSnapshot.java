package in.gov.uidai.dp.velocity.engine.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

/**
 * Compact snapshot of a {@link VelocityRule} stored in keyed {@code ValueState}.
 *
 * <p>The full {@link VelocityRule} cannot be accessed from {@code onTimer} because
 * broadcast state is not available in the timer context of a
 * {@code KeyedBroadcastProcessFunction}. Instead, on every {@code processElement} call
 * we update this snapshot from the current broadcast state, and timers read it from
 * keyed state.
 *
 * <p>Only the fields needed for window evaluation and alert emission are stored,
 * minimising serialised size in RocksDB.
 */
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

    /**
     * Factory method — extract only what's needed from a full rule.
     */
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