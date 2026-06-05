package in.gov.uidai.dp.velocity.engine.utils;

import in.gov.uidai.dp.velocity.engine.model.Event;
import in.gov.uidai.dp.velocity.engine.model.FilterNode;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public final class FilterEvaluator {

    private FilterEvaluator() {}

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
            return isAnd; // AND: all passed → true; OR: none passed → false
        }

        if (filterRoot.isCondition()) {
            return evaluateCondition(event, filterRoot);
        }

        log.warn("Unknown filter node type: {}", filterRoot.getType());
        return true;
    }

    private static boolean evaluateCondition(Event event, FilterNode cond) {
        Object eventVal = FieldExtractor.extractObject(event, cond.getField());
        Object condVal = cond.getValue();
        String operator = cond.getOperator();

        if (eventVal == null) {
            return "NOT_EQUALS".equalsIgnoreCase(operator);
        }

        try {
            return switch (operator.toUpperCase()) {
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