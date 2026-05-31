package in.gov.uidai.dp.velocity.engine.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class AggregationSpec implements Serializable {

    private static final long serialVersionUID = 1L;

    private String alias;

    private String field;

    private String function;

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