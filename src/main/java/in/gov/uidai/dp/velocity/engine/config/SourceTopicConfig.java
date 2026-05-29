package in.gov.uidai.dp.velocity.engine.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data @NoArgsConstructor @JsonIgnoreProperties(ignoreUnknown = true)
public class SourceTopicConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    private String topic;
    @JsonProperty("group-id")       private String groupId;
    private String offset = "latest";
    @JsonProperty("event-timestamp-field")  private String eventTimestampField  = "_event_timestamp";
    @JsonProperty("event-timestamp-format") private String eventTimestampFormat = "ISO_STRING";
    @JsonProperty("allowed-lateness-ms")    private long   allowedLatenessMs    = 0L;
}