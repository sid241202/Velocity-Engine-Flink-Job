package in.gov.uidai.dp.velocity.engine.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data @NoArgsConstructor @AllArgsConstructor @JsonIgnoreProperties(ignoreUnknown = true)
public class HavingCondition implements Serializable {
    private static final long serialVersionUID = 1L;

    /** References the alias of an {@link AggregationSpec}. */
    @JsonProperty("alias_ref")
    private String aliasRef;

    /** EQUALS | NOT_EQUALS | GREATER_THAN | GREATER_THAN_EQUAL | LESS_THAN | LESS_THAN_EQUAL */
    private String operator;

    private double value;
}