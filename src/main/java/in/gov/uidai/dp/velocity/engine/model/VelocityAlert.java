package in.gov.uidai.dp.velocity.engine.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Map;

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

    private String windowStart;

    private String windowEnd;

    private String aggregationResults;

    private String triggeredAt;
}