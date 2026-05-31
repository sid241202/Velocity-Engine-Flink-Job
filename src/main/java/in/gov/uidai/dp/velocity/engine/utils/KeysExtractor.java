package in.gov.uidai.dp.velocity.engine.utils;

import in.gov.uidai.dp.velocity.engine.model.Event;

import java.util.List;

public final class KeysExtractor {

    private KeysExtractor() {}

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