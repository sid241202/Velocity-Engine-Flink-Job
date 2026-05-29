package in.gov.uidai.dp.velocity.engine.utils;

import in.gov.uidai.dp.velocity.engine.model.HavingCondition;
import in.gov.uidai.dp.velocity.engine.model.HavingThresholds;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

/**
 * Evaluates {@link HavingThresholds} against a map of alias → computed window value.
 * Returns true if the threshold condition is breached (i.e. an alert should fire).
 */
@Slf4j
public final class HavingEvaluator {

    private HavingEvaluator() {}

    /**
     * @param thresholds  rule's having thresholds config
     * @param aliasValues map of alias name → computed aggregation value for the current window
     * @return true if threshold is breached and an alert should fire
     */
    public static boolean evaluate(HavingThresholds thresholds, Map<String, Double> aliasValues) {
        if (thresholds == null || thresholds.getConditions() == null
                || thresholds.getConditions().isEmpty()) {
            return false; // no threshold configured → never alert
        }

        boolean isAnd = thresholds.isAnd();

        for (HavingCondition cond : thresholds.getConditions()) {
            double actual  = aliasValues.getOrDefault(cond.getAliasRef(), 0.0);
            boolean result = compare(actual, cond.getOperator(), cond.getValue());

            if (isAnd && !result) return false; // AND: one failure = overall false
            if (!isAnd && result) return true;  // OR:  one success = overall true
        }

        return isAnd; // AND: all passed = true; OR: none passed = false
    }

    private static boolean compare(double actual, String operator, double threshold) {
        return switch (operator.toUpperCase()) {
            case "EQUALS"             -> actual == threshold;
            case "NOT_EQUALS"         -> actual != threshold;
            case "GREATER_THAN"       -> actual >  threshold;
            case "GREATER_THAN_EQUAL" -> actual >= threshold;
            case "LESS_THAN"          -> actual <  threshold;
            case "LESS_THAN_EQUAL"    -> actual <= threshold;
            default -> {
                log.warn("Unknown having operator '{}' — treating as not breached", operator);
                yield false;
            }
        };
    }
}