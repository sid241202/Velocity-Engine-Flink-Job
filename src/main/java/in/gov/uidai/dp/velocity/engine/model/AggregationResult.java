package in.gov.uidai.dp.velocity.engine.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Map;

/**
 * Output record emitted for every window evaluation, regardless of whether the
 * having threshold was breached. Written to ClickHouse for analytics dashboards.
 *
 * <p>All datetime strings are formatted in IST (Asia/Kolkata),
 * pattern: {@code yyyy-MM-dd'T'HH:mm:ss}.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class AggregationResult implements Serializable {

    private static final long serialVersionUID = 1L;

    private String ruleId;
    private String ruleName;
    private String sourceTopic;
    private String cluster;

    /** Composite grouping key, e.g. {@code "enrolRef123|device456"} */
    private String groupKey;

    /** Window start in IST, e.g. {@code "2026-05-25T08:43:00"} */
    private String windowStart;

    /** Window end in IST */
    private String windowEnd;

    /** SLIDING | TUMBLING */
    private String windowType;

    /** EVENT_TIME | PROCESSING_TIME */
    private String timeType;

    /**
     * Map of computed aggregation values by alias.
     * Stored natively as ClickHouse Map(String, Float64).
     */
    private Map<String, Double> aggregationResults;

    /** 1 if having thresholds were breached, 0 otherwise. ClickHouse UInt8. */
    private int thresholdBreached;

    /** HIGH | MEDIUM | LOW */
    private String severityLevel;

    /** Number of raw events that contributed to this window. */
    private long eventCount;

    /** Evaluation time in IST. */
    private String evaluatedAt;
}