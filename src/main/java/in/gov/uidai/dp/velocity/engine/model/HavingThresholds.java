package in.gov.uidai.dp.velocity.engine.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

@Data
@NoArgsConstructor @AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class HavingThresholds implements Serializable {
    private static final long serialVersionUID = 1L;

    @JsonProperty("logic_operator")
private String logicOperator;

    private List<HavingCondition> conditions;

    public boolean isAnd() {
        return "AND".equalsIgnoreCase(logicOperator);
    }
}