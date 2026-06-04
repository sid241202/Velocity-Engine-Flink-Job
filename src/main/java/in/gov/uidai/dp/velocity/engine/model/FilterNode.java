package in.gov.uidai.dp.velocity.engine.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

import org.apache.flink.api.common.typeinfo.TypeInfo;

@TypeInfo(FilterNodeTypeInfoFactory.class)
@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class FilterNode implements Serializable {
    private static final long serialVersionUID = 1L;

    private String type; // "condition" or "group"
    private String logic; // "AND" or "OR" (for groups only)
    private List<FilterNode> conditions; // children (for groups only)

    // For condition nodes:
    private String field;
    private String operator; // EQUALS, NOT_EQUALS, GREATER_THAN, GREATER_THAN_EQUAL, LESS_THAN, LESS_THAN_EQUAL, IN, REGEX
    private Object value;

    public boolean isGroup() {
        return "group".equalsIgnoreCase(type);
    }

    public boolean isCondition() {
        return "condition".equalsIgnoreCase(type);
    }

    public boolean isAnd() {
        return "AND".equalsIgnoreCase(logic);
    }
}
