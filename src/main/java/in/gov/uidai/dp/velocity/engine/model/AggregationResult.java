package in.gov.uidai.dp.velocity.engine.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AggregationResult implements Serializable {
    private static final long serialVersionUID = 3L;
    private String id;
    private String windowStart;
    private String windowEnd;
    private String entityName;
    // groupKey is the resolved grouping value (composite of groupBy field values).
    // Sent explicitly so the frontend and ClickHouse sink never have to guess from entityValue.
    private String groupKey;
    private String entityValue; // kept for backward compatibility
    private String aggResult;
    private String producedAt;
    // thresholdBreached is the canonical breach flag. Set to true by RuleEvaluatorFunction
    // when the HAVING clause evaluates to true for the closed window.
    // The backend consumer and frontend both read this field to determine breach status.
    private boolean thresholdBreached;
}