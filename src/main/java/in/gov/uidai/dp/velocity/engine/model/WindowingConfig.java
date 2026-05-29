package in.gov.uidai.dp.velocity.engine.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Windowing configuration for a rule.
 *
 * <p>Two mechanisms work together:
 * <ol>
 *   <li><b>Watermark-based event ordering</b> — uses {@code allowedLatenessMs} as
 *       {@code forBoundedOutOfOrderness} bound at the source level.</li>
 *   <li><b>Processing-time timers in RuleEvaluatorFunction</b> — fire every
 *       {@code effectiveSlideMs} regardless of event flow, guaranteeing windows
 *       always trigger even during idle periods.</li>
 * </ol>
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class WindowingConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    /** SLIDING | TUMBLING */
    private String type;

    /** EVENT_TIME | PROCESSING_TIME */
    @JsonProperty("time_type")
    private String timeType;

    /**
     * Field in {@link Event#fields} that holds the epoch-millisecond timestamp.
     * Injected by {@link in.gov.uidai.dp.velocity.engine.deserializers.EventDeserializer}
     * as {@code _event_timestamp_epoch_ms}. Can be overridden per rule to use
     * a different timestamp field from _data.
     */
    @JsonProperty("timestamp_field")
    private String timestampField = "_event_timestamp_epoch_ms";

    /** Window size in milliseconds. */
    @JsonProperty("size_ms")
    private long sizeMs;

    /**
     * Slide interval in milliseconds (only relevant for SLIDING windows).
     * Also the period at which processing-time timers fire for periodic evaluation.
     */
    @JsonProperty("slide_ms")
    private long slideMs;

    /**
     * Out-of-orderness bound for watermark strategy (set per source topic in jobconf.yaml).
     * Stored here so rule-level timers can factor it in when pruning state.
     */
    @JsonProperty("allowed_lateness_ms")
    private long allowedLatenessMs = 0L;

    public boolean isEventTime() {
        return "EVENT_TIME".equalsIgnoreCase(timeType);
    }

    public boolean isSliding() {
        return "SLIDING".equalsIgnoreCase(type);
    }

    /**
     * Returns the effective slide interval.
     * For TUMBLING windows, slide = size (no overlap).
     */
    public long getEffectiveSlideMs() {
        return isSliding() ? slideMs : sizeMs;
    }

    /**
     * Returns the effective timestamp field, defaulting to the injected epoch ms field.
     */
    public String getEffectiveTimestampField() {
        return (timestampField != null && !timestampField.isBlank())
                ? timestampField
                : "_event_timestamp_epoch_ms";
    }
}