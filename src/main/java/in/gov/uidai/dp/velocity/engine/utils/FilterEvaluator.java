package in.gov.uidai.dp.velocity.engine.utils;

import in.gov.uidai.dp.velocity.engine.model.Event;
import in.gov.uidai.dp.velocity.engine.model.FilterCondition;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * Evaluates a list of {@link FilterCondition}s against an {@link Event}.
 * All conditions must pass (implicit AND between conditions).
 * Short-circuits on first failure for efficiency.
 */
@Slf4j
public final class FilterEvaluator {

    private FilterEvaluator() {}

    /**
     * @return true if all conditions pass (or filters is null/empty), false otherwise
     */
    public static boolean evaluate(Event event, List<FilterCondition> filters) {
        if (filters == null || filters.isEmpty()) return true;
        for (FilterCondition condition : filters) {
            if (!evaluateOne(event, condition)) return false;
        }
        return true;
    }

    private static boolean evaluateOne(Event event, FilterCondition cond) {
        Object eventVal = FieldExtractor.extractObject(event, cond.getField());
        Object condVal  = cond.getValue();
        String operator = cond.getOperator();

        if (eventVal == null) {
            return "NOT_EQUALS".equalsIgnoreCase(operator);
        }

        try {
            return switch (operator.toUpperCase()) {
                case "EQUALS"              -> strOf(eventVal).equalsIgnoreCase(strOf(condVal));
                case "NOT_EQUALS"          -> !strOf(eventVal).equalsIgnoreCase(strOf(condVal));
                case "GREATER_THAN"        -> toDouble(eventVal) >  toDouble(condVal);
                case "GREATER_THAN_EQUAL"  -> toDouble(eventVal) >= toDouble(condVal);
                case "LESS_THAN"           -> toDouble(eventVal) <  toDouble(condVal);
                case "LESS_THAN_EQUAL"     -> toDouble(eventVal) <= toDouble(condVal);
                case "IN"                  -> evaluateIn(eventVal, condVal);
                case "REGEX"               -> strOf(eventVal).matches(strOf(condVal));
                default -> {
                    log.warn("Unknown filter operator '{}' — treating as no-match", operator);
                    yield false;
                }
            };
        } catch (Exception e) {
            log.debug("Filter evaluation error for field '{}' op '{}': {}",
                    cond.getField(), operator, e.getMessage());
            return false;
        }
    }

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