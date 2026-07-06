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
    private String groupKey;
    private String entityValue;
    private String aggResult;
    private String producedAt;
    private boolean thresholdBreached;
    /** true = authoritative end-of-window row (state pruned); false = early-fire preview, superseded later. */
    private boolean isFinal;
}