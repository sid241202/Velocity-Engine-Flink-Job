package in.gov.uidai.dp.velocity.engine.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
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

    private String type;
    private String logic;
    private List<FilterNode> conditions;
    private String field;
    private String operator;
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
