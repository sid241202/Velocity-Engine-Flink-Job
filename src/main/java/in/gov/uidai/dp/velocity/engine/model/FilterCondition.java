package in.gov.uidai.dp.velocity.engine.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * A single pre-filter condition evaluated against every incoming event
 * before rule grouping and aggregation.
 *
 * <p>Supported operators: EQUALS, NOT_EQUALS, GREATER_THAN, GREATER_THAN_EQUAL,
 * LESS_THAN, LESS_THAN_EQUAL, IN (value is a List), REGEX.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class FilterCondition implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Dot-notation field path, e.g. {@code _data.authResult} or {@code _event_type}. */
    private String field;

    /** Operator string: EQUALS | NOT_EQUALS | GREATER_THAN | GREATER_THAN_EQUAL |
     *  LESS_THAN | LESS_THAN_EQUAL | IN | REGEX */
    private String operator;

    /**
     * Comparison value. Jackson deserializes this as:
     * <ul>
     *   <li>String for EQUALS, NOT_EQUALS, REGEX</li>
     *   <li>Number for numeric comparisons</li>
     *   <li>List&lt;String&gt; for IN operator</li>
     * </ul>
     */
    private Object value;
}