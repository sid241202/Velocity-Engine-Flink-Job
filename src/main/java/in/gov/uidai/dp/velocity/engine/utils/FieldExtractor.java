package in.gov.uidai.dp.velocity.engine.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import in.gov.uidai.dp.velocity.engine.model.Event;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

@Slf4j
public final class FieldExtractor {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};

    private FieldExtractor() {}

    public static String extractString(Event event, String fieldPath) {
        Object val = extractObject(event, fieldPath);
        return val != null ? String.valueOf(val) : null;
    }

    public static Double extractDouble(Event event, String fieldPath) {
        Object val = extractObject(event, fieldPath);
        if (val == null) return null;
        if (val instanceof Number) return ((Number) val).doubleValue();
        try {
            return Double.parseDouble(String.valueOf(val));
        } catch (NumberFormatException e) {
            log.debug("Cannot parse '{}' as double from field '{}'", val, fieldPath);
            return null;
        }
    }

    public static Object extractObject(Event event, String fieldPath) {
        if (event == null || fieldPath == null || fieldPath.isBlank()) return null;

        if (fieldPath.startsWith("_data.")) {

            Map<String, Object> data = parseDataIfNeeded(event);
            if (data == null) return null;

            String subPath = fieldPath.substring("_data.".length());
            return navigate(data, subPath);
        }

        return event.getFields().get(fieldPath);
    }

    @SuppressWarnings("unchecked")
private static Map<String, Object> parseDataIfNeeded(Event event) {
        if (event.parsedData != null) return event.parsedData;

        Object rawData = event.getFields().get("_data");
        if (rawData == null) return null;

        if (rawData instanceof Map) {
            event.parsedData = (Map<String, Object>) rawData;
            return event.parsedData;
        }

        try {
            event.parsedData = MAPPER.readValue(rawData.toString(), MAP_TYPE);
            return event.parsedData;
        } catch (Exception e) {
            log.warn("Failed to parse _data JSON: {}", e.getMessage());
            return null;
        }
    }

    @SuppressWarnings("unchecked")
private static Object navigate(Map<String, Object> map, String dotPath) {
        String[] parts = dotPath.split("\\.", 2);
        Object val = map.get(parts[0]);
        if (val == null || parts.length == 1) return val;
        if (val instanceof Map) {
            return navigate((Map<String, Object>) val, parts[1]);
        }
        return null;
    }
}