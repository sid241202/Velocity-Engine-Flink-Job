package in.gov.uidai.dp.velocity.engine.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

import org.apache.flink.api.common.typeinfo.TypeInfo;

/**
 * FilterNode — represents a single condition or a logical group of conditions.
 *
 * <p>Supported operators (case-insensitive):
 * <ul>
 *   <li>Standard: EQUALS, NOT_EQUALS, GREATER_THAN, GREATER_THAN_EQUAL,
 *       LESS_THAN, LESS_THAN_EQUAL, IN, REGEX, CONTAINS, NOT_CONTAINS,
 *       STARTS_WITH, ENDS_WITH</li>
 *   <li>Null checks: IS_NULL, IS_NOT_NULL  — value field is ignored</li>
 *   <li>Date/Time:  DATE_BEFORE, DATE_AFTER, DATE_EQUALS
 *       — value is compared as epoch-ms or ISO-8601 depending on {@code format}</li>
 * </ul>
 *
 * <p>The {@code format} field is used only by date operators:
 * <ul>
 *   <li>"EPOCH_MILLIS" — field value is a Unix timestamp in milliseconds (long)</li>
 *   <li>"ISO_STRING"   — field value is an ISO-8601 date-time string</li>
 * </ul>
 * If {@code format} is absent or the field cannot be parsed, the condition silently
 * returns {@code false} — the Flink job never crashes on bad user input.
 */
@TypeInfo(FilterNodeTypeInfoFactory.class)
@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class FilterNode implements Serializable {
    private static final long serialVersionUID = 2L;

    private String type;
    private String logic;
    private List<FilterNode> conditions;
    private String field;
    private String operator;
    private Object value;

    /**
     * Optional format hint for date/time operators.
     * Valid values: "EPOCH_MILLIS" | "ISO_STRING"
     * Ignored for all other operators.
     */
    @JsonProperty("format")
    private String format;

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
