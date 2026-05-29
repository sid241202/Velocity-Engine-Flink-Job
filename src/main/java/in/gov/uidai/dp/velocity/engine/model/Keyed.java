package in.gov.uidai.dp.velocity.engine.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.io.Serializable;

/**
 * Wrapper that pairs an event with its composite grouping key and the matching rule ID.
 * Emitted by {@link in.gov.uidai.dp.velocity.engine.functions.DynamicKeyFunction}
 * and consumed by {@link in.gov.uidai.dp.velocity.engine.functions.RuleEvaluatorFunction}.
 *
 * @param <IN>  wrapped event type (typically {@link Event})
 * @param <KEY> composite grouping key type (typically {@link String})
 * @param <ID>  rule identifier type (typically {@link String})
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class Keyed<IN, KEY, ID> implements Serializable {

    private static final long serialVersionUID = 1L;

    private IN  wrapped;
    private KEY key;
    private ID  id;
}