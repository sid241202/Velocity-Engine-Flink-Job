package in.gov.uidai.dp.velocity.engine.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AggregationResult implements Serializable {

    private static final long serialVersionUID = 1L;

    private String ruleId;
    private String ruleName;
    private String sourceTopic;
    private String cluster;

    private String groupKey;

    private String windowStart;

    private String windowEnd;

    private String windowType;

    private String timeType;

    private Map<String, Double> aggregationResults;

    private int thresholdBreached;

    private String severityLevel;

    private long eventCount;

    private String evaluatedAt;
}