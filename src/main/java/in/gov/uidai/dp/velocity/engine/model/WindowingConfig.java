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
public class WindowingConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private String type;

    @JsonProperty("time_type")
private String timeType;

    @JsonProperty("timestamp_field")
private String timestampField = "_event_timestamp_epoch_ms";

    @JsonProperty("timestamp_format")
private String timestampFormat = "EPOCH_MILLIS";

    @JsonProperty("size_ms")
private long sizeMs;

    @JsonProperty("slide_ms")
private long slideMs;

    @JsonProperty("allowed_lateness_ms")
private long allowedLatenessMs = 0L;

    public boolean isEventTime() {
        return "EVENT_TIME".equalsIgnoreCase(timeType);
    }

    public boolean isSliding() {
        return "SLIDING".equalsIgnoreCase(type);
    }

    public long getEffectiveSlideMs() {
        return isSliding() ? slideMs : sizeMs;
    }

    public String getEffectiveTimestampField() {
        return (timestampField != null && !timestampField.isBlank())
                ? timestampField
                : "_event_timestamp_epoch_ms";
    }
}