package in.gov.uidai.dp.velocity.engine.model;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

public class Event implements Serializable {

    private static final long serialVersionUID = 1L;

    @JsonIgnore
    public final Map<String, Object> fields = new LinkedHashMap<>();

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