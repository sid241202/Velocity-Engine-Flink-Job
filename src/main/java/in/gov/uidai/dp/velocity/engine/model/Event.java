package in.gov.uidai.dp.velocity.engine.model;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Generic, schema-agnostic event container backed by a {@link LinkedHashMap}.
 *
 * <p>All Kafka event fields are stored as key-value pairs. The {@code _data} sub-object
 * is intentionally kept as a raw JSON String by {@link in.gov.uidai.dp.velocity.engine.deserializers.EventDeserializer}
 * and only parsed on demand (lazy) by {@link in.gov.uidai.dp.velocity.engine.utils.FieldExtractor}.
 *
 * <p>Injected fields (added by deserializer, not in original event):
 * <ul>
 *   <li>{@code _source_topic} — Kafka topic name, used for rule routing</li>
 *   <li>{@code _cluster} — cluster identifier</li>
 *   <li>{@code _event_timestamp_epoch_ms} — epoch millis timestamp, used for watermarks</li>
 * </ul>
 */
public class Event implements Serializable {

    private static final long serialVersionUID = 1L;

    @JsonIgnore
    public final Map<String, Object> fields = new LinkedHashMap<>();

    /**
     * Transient cache for lazily parsed {@code _data} field.
     * Not serialized — re-parsed on demand if evicted from memory.
     */
    @JsonIgnore
    public transient Map<String, Object> parsedData = null;

    @JsonAnySetter
    public void put(String name, Object value) {
        fields.put(name, value);
    }

    @JsonAnyGetter
    public Map<String, Object> any() {
        return new LinkedHashMap<>(fields);
    }

    @JsonIgnore
    public Map<String, Object> getFields() {
        return fields;
    }

    @Override
    public String toString() {
        return "Event" + fields.toString();
    }
}