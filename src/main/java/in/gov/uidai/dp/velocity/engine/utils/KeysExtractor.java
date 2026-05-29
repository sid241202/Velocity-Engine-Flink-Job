package in.gov.uidai.dp.velocity.engine.utils;

import in.gov.uidai.dp.velocity.engine.model.Event;

import java.util.List;

/**
 * Builds the composite grouping key from an event by extracting and joining
 * the values of the configured grouping field paths.
 *
 * <p>Example: keys = ["_data.enrolmentReferenceId", "_data.deviceCode"]
 * produces key = "abc123|device456". Null values are represented as "NULL".
 */
public final class KeysExtractor {

    private KeysExtractor() {}

    /**
     * Build composite grouping key.
     *
     * @param fieldPaths  list of dot-notation field paths from rule grouping config
     * @param event       the event to extract values from
     * @return pipe-delimited composite key string
     */
    public static String getKey(List<String> fieldPaths, Event event) {
        if (fieldPaths == null || fieldPaths.isEmpty()) return "ALL";

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < fieldPaths.size(); i++) {
            String val = FieldExtractor.extractString(event, fieldPaths.get(i));
            sb.append(val != null ? val : "NULL");
            if (i < fieldPaths.size() - 1) sb.append('|');
        }
        return sb.toString();
    }
}