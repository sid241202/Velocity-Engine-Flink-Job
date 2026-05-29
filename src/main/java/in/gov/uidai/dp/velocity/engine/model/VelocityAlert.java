package in.gov.uidai.dp.velocity.engine.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Anomaly alert emitted when a rule's having thresholds are breached.
 * Written to KeyDB with TTL = {@code penaltyTtlSeconds}.
 *
 * <p>KeyDB key pattern: {@code anomaly:{ruleId}:{groupKey}}
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class VelocityAlert implements Serializable {

    private static final long serialVersionUID = 1L;

    private String ruleId;
    private String ruleName;
    private String severityLevel;
    private int    penaltyTtlSeconds;
    private String groupKey;
    private String sourceTopic;
    private String cluster;

    /** Window start in IST. */
    private String windowStart;

    /** Window end in IST. */
    private String windowEnd;

    /**
     * JSON of alias → computed value for all aggregations.
     * E.g. {@code {"unique_aua_count":7,"total_txns":12}}
     */
    private String aggregationResults;

    /** Time at which the alert was triggered, in IST. */
    private String triggeredAt;
}