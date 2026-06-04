package in.gov.uidai.dp.velocity.engine.utils;

import in.gov.uidai.dp.velocity.engine.model.HavingThresholds;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.jexl3.*;

import java.util.Map;

@Slf4j
public final class HavingEvaluator {

    private static final JexlEngine JEXL = new JexlBuilder()
            .strict(false)
            .silent(true)
            .create();

    private HavingEvaluator() {}

    public static boolean evaluate(HavingThresholds thresholds, Map<String, Double> aliasValues) {
        if (thresholds == null || thresholds.getExpression() == null || thresholds.getExpression().isBlank()) {
            return false;
        }
        try {
            JexlExpression expr = JEXL.createExpression(thresholds.getExpression());
            JexlContext ctx = new MapContext();
            aliasValues.forEach(ctx::set);
            Object result = expr.evaluate(ctx);
            if (result instanceof Boolean) {
                return (Boolean) result;
            }
            log.warn("JEXL expression '{}' did not return boolean, got: {}", thresholds.getExpression(), result);
            return false;
        } catch (Exception e) {
            log.error("Failed to evaluate JEXL expression '{}': {}", thresholds.getExpression(), e.getMessage());
            return false;
        }
    }
}