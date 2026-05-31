package in.gov.uidai.dp.velocity.engine.utils;

import in.gov.uidai.dp.velocity.engine.model.HavingCondition;
import in.gov.uidai.dp.velocity.engine.model.HavingThresholds;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

@Slf4j
public final class HavingEvaluator {

    private HavingEvaluator() {}

    public static boolean evaluate(HavingThresholds thresholds, Map<String, Double> aliasValues) {
        if (thresholds == null || thresholds.getConditions() == null
                || thresholds.getConditions().isEmpty()) {
            return false;
        }

        boolean isAnd = thresholds.isAnd();

        for (HavingCondition cond : thresholds.getConditions()) {
            double actual  = aliasValues.getOrDefault(cond.getAliasRef(), 0.0);
            boolean result = compare(actual, cond.getOperator(), cond.getValue());

            if (isAnd && !result) return false;
            if (!isAnd && result) return true;
        }

        return isAnd;
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