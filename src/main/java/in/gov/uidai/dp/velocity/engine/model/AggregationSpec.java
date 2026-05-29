package in.gov.uidai.dp.velocity.engine.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Specifies a single aggregation to compute within a rule's window.
 * Multiple aggregation specs per rule are supported.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class AggregationSpec implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Alias used to reference this aggregation in {@link HavingCondition#aliasRef}. */
    private String alias;

    /** Dot-notation field path to aggregate over. E.g. {@code _data.aua}. */
    private String field;

    /** Aggregation function: COUNT | SUM | AVG | MIN | MAX | COUNT_DISTINCT */
    private String function;

    /**
     * Cardinality hint for COUNT_DISTINCT.
     * <ul>
     *   <li>LOW (default): exact HashSet — suitable when distinct values per group key
     *       within the window are < 10k (e.g. distinct AUAs used by one resident).</li>
     *   <li>HIGH: HyperLogLog sketch (±2% error) — suitable when one group key can
     *       accumulate tens of thousands of distinct values (e.g. distinct residents
     *       per AUA code).</li>
     * </ul>
     */
    @JsonProperty("cardinality_hint")
    private String cardinalityHint = "LOW";

    public boolean isCountDistinct() {
        return "COUNT_DISTINCT".equalsIgnoreCase(function);
    }

    public boolean isHighCardinality() {
        return "HIGH".equalsIgnoreCase(cardinalityHint);
    }

    public boolean isCount() { return "COUNT".equalsIgnoreCase(function); }
    public boolean isSum()   { return "SUM".equalsIgnoreCase(function); }
    public boolean isAvg()   { return "AVG".equalsIgnoreCase(function); }
    public boolean isMin()   { return "MIN".equalsIgnoreCase(function); }
    public boolean isMax()   { return "MAX".equalsIgnoreCase(function); }
}