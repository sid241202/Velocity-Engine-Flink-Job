package in.gov.uidai.dp.velocity.engine.utils;

import in.gov.uidai.dp.velocity.engine.model.Event;
import in.gov.uidai.dp.velocity.engine.model.FilterNode;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.List;

/**
 * FilterEvaluator — stateless, exception-safe evaluation of {@link FilterNode} trees
 * against incoming {@link Event} payloads.
 *
 * <h3>Design Principles</h3>
 * <ul>
 *   <li>All evaluation logic is wrapped in try-catch blocks. A bad filter (wrong
 *       format, missing field, invalid operator) silently returns {@code false}
 *       — the Flink job never crashes on user misconfiguration.</li>
 *   <li>IS_NULL / IS_NOT_NULL: checks whether the field is missing from the event.</li>
 *   <li>DATE_BEFORE / DATE_AFTER / DATE_EQUALS: parses field and condition value
 *       according to the {@code format} hint (EPOCH_MILLIS or ISO_STRING). Wrong
 *       format → {@code false}.</li>
 * </ul>
 */
@Slf4j
public final class FilterEvaluator {

    private FilterEvaluator() {}

    // ─── Public entry point ───────────────────────────────────────────────────

    public static boolean evaluate(Event event, FilterNode filterRoot) {
        if (filterRoot == null) return true;

        if (filterRoot.isGroup()) {
            List<FilterNode> children = filterRoot.getConditions();
            if (children == null || children.isEmpty()) return true;

            boolean isAnd = filterRoot.isAnd();
            for (FilterNode child : children) {
                boolean result = evaluate(event, child);
                if (isAnd && !result) return false;
                if (!isAnd && result) return true;
            }
            return isAnd;
        }

        if (filterRoot.isCondition()) {
            return evaluateCondition(event, filterRoot);
        }

        log.warn("Unknown filter node type: {}", filterRoot.getType());
        return true;
    }

    // ─── Condition dispatch ───────────────────────────────────────────────────

    private static boolean evaluateCondition(Event event, FilterNode cond) {
        String operator = cond.getOperator();
        if (operator == null || operator.isBlank()) {
            log.warn("Filter condition has null/blank operator for field '{}' — skipping", cond.getField());
            return true;
        }

        String opUpper = operator.toUpperCase();

        // ── Null checks (don't need eventVal) ────────────────────────────────
        if ("IS_NULL".equals(opUpper)) {
            Object eventVal = FieldExtractor.extractObject(event, cond.getField());
            return eventVal == null;
        }
        if ("IS_NOT_NULL".equals(opUpper)) {
            Object eventVal = FieldExtractor.extractObject(event, cond.getField());
            return eventVal != null;
        }

        // ── Date/Time comparisons ─────────────────────────────────────────────
        if ("DATE_BEFORE".equals(opUpper) || "DATE_AFTER".equals(opUpper) || "DATE_EQUALS".equals(opUpper)) {
            return evaluateDateCondition(event, cond, opUpper);
        }

        // ── Standard comparisons ──────────────────────────────────────────────
        Object eventVal = FieldExtractor.extractObject(event, cond.getField());
        Object condVal  = cond.getValue();

        // Field missing from event: only NOT_EQUALS passes
        if (eventVal == null) {
            return "NOT_EQUALS".equalsIgnoreCase(operator);
        }

        try {
            return switch (opUpper) {
                case "EQUALS" -> {
                    String eStr = strOf(eventVal);
                    String cStr = strOf(condVal);
                    if (eStr.equalsIgnoreCase(cStr)) yield true;
                    try {
                        yield Double.parseDouble(eStr) == Double.parseDouble(cStr);
                    } catch (NumberFormatException ignore) {
                        yield false;
                    }
                }
                case "NOT_EQUALS" -> {
                    String eStr = strOf(eventVal);
                    String cStr = strOf(condVal);
                    if (eStr.equalsIgnoreCase(cStr)) yield false;
                    try {
                        yield Double.parseDouble(eStr) != Double.parseDouble(cStr);
                    } catch (NumberFormatException ignore) {
                        yield true;
                    }
                }
                case "GREATER_THAN"       -> toDouble(eventVal) >  toDouble(condVal);
                case "GREATER_THAN_EQUAL" -> toDouble(eventVal) >= toDouble(condVal);
                case "LESS_THAN"          -> toDouble(eventVal) <  toDouble(condVal);
                case "LESS_THAN_EQUAL"    -> toDouble(eventVal) <= toDouble(condVal);
                case "IN"                 -> evaluateIn(eventVal, condVal);
                case "REGEX"              -> strOf(eventVal).matches(strOf(condVal));
                case "CONTAINS"           -> strOf(eventVal).toLowerCase().contains(strOf(condVal).toLowerCase());
                case "NOT_CONTAINS"       -> !strOf(eventVal).toLowerCase().contains(strOf(condVal).toLowerCase());
                case "STARTS_WITH"        -> strOf(eventVal).toLowerCase().startsWith(strOf(condVal).toLowerCase());
                case "ENDS_WITH"          -> strOf(eventVal).toLowerCase().endsWith(strOf(condVal).toLowerCase());
                default -> {
                    log.warn("Unknown filter operator '{}' — treating as pass", operator);
                    yield true;
                }
            };
        } catch (Exception e) {
            log.debug("Filter evaluation error for field '{}' op '{}': {}", cond.getField(), operator, e.getMessage());
            return false;
        }
    }

    // ─── Date / Time evaluation ───────────────────────────────────────────────

    /**
     * Evaluates DATE_BEFORE / DATE_AFTER / DATE_EQUALS.
     *
     * <p>Both the event field value and the condition value are parsed into epoch-ms
     * according to the {@code format} hint on the condition node:
     * <ul>
     *   <li>"EPOCH_MILLIS" (default) — parse as {@code long}</li>
     *   <li>"ISO_STRING"   — parse as ISO-8601 instant</li>
     * </ul>
     * Any parse error returns {@code false} silently so the job never crashes.
     */
    private static boolean evaluateDateCondition(Event event, FilterNode cond, String opUpper) {
        try {
            Object rawField = FieldExtractor.extractObject(event, cond.getField());
            Object rawCond  = cond.getValue();

            if (rawField == null || rawCond == null) {
                log.debug("Date filter: field or condition value is null for field '{}' — returning false", cond.getField());
                return false;
            }

            String format = cond.getFormat();
            boolean isIso = "ISO_STRING".equalsIgnoreCase(format);

            long eventMs = parseToEpochMs(rawField, isIso, cond.getField());
            long condMs  = parseToEpochMs(rawCond,  isIso, "conditionValue");

            if (eventMs < 0 || condMs < 0) {
                // Parse failed — silently return false
                return false;
            }

            return switch (opUpper) {
                case "DATE_BEFORE" -> eventMs < condMs;
                case "DATE_AFTER"  -> eventMs > condMs;
                case "DATE_EQUALS" -> eventMs == condMs;
                default -> false;
            };
        } catch (Exception e) {
            // Fault-tolerant: any unexpected error → false, never crash
            log.debug("Date filter evaluation error for field '{}' op '{}': {}", cond.getField(), opUpper, e.getMessage());
            return false;
        }
    }

    /**
     * Parses a raw value to epoch-milliseconds.
     *
     * @param raw   the raw field/condition value
     * @param isIso true → parse as ISO-8601 string; false → parse as numeric millis
     * @param label label for debug logging only
     * @return epoch-ms, or {@code -1} if parsing failed
     */
    private static long parseToEpochMs(Object raw, boolean isIso, String label) {
        String s = strOf(raw).trim();
        if (s.isEmpty()) return -1L;

        if (isIso) {
            try {
                return Instant.parse(s).toEpochMilli();
            } catch (DateTimeParseException e1) {
                // Try without timezone (assume IST / local)
                try {
                    return java.time.LocalDateTime.parse(s, java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME)
                            .atZone(TimeUtils.INDIA_ZONE)
                            .toInstant()
                            .toEpochMilli();
                } catch (Exception e2) {
                    log.debug("ISO_STRING parse failed for '{}' value='{}': {}", label, s, e2.getMessage());
                    return -1L;
                }
            }
        } else {
            // EPOCH_MILLIS (default)
            try {
                if (raw instanceof Number) return ((Number) raw).longValue();
                return Long.parseLong(s);
            } catch (NumberFormatException e) {
                log.debug("EPOCH_MILLIS parse failed for '{}' value='{}': {}", label, s, e.getMessage());
                return -1L;
            }
        }
    }

    // ─── Helpers ─────────────────────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    private static boolean evaluateIn(Object eventVal, Object condVal) {
        if (!(condVal instanceof List)) return false;
        String evStr = strOf(eventVal).toLowerCase();
        for (Object item : (List<?>) condVal) {
            if (evStr.equals(strOf(item).toLowerCase())) return true;
        }
        return false;
    }

    private static String strOf(Object o) {
        return o != null ? String.valueOf(o) : "";
    }

    private static double toDouble(Object o) {
        if (o instanceof Number) return ((Number) o).doubleValue();
        return Double.parseDouble(String.valueOf(o));
    }
}