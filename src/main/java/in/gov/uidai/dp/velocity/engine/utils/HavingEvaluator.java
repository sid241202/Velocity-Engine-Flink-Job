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

    /**
     * Evaluates the threshold expression against raw event fields (no-windowing mode).
     * A blank/null expression returns FALSE, matching {@link #evaluate}'s established
     * semantics: no threshold configured means "data-only rule" (the frontend's own
     * rule-summary copy says exactly this, for windowed and no-windowing rules alike —
     * see RuleSummaryPanel.jsx's thresholdDesc). A caller who genuinely wants "fire on
     * every matching event, no threshold" can express that explicitly with a JEXL
     * expression that's always true (e.g. "true") rather than relying on blank meaning
     * something different here than it does for windowed rules.
     */
    public static boolean evaluateRaw(HavingThresholds thresholds, Map<String, Object> rawFields) {
        if (thresholds == null || thresholds.getExpression() == null || thresholds.getExpression().isBlank()) {
            return false; // blank expression = data-only rule, same as the windowed path
        }
        try {
            JexlExpression expr = JEXL.createExpression(thresholds.getExpression());
            JexlContext ctx = new MapContext();
            if (rawFields != null) rawFields.forEach(ctx::set);
            Object result = expr.evaluate(ctx);
            if (result instanceof Boolean) return (Boolean) result;
            log.warn("JEXL raw expression '{}' did not return boolean, got: {}", thresholds.getExpression(), result);
            return false;
        } catch (Exception e) {
            log.error("Failed to evaluate raw JEXL expression '{}': {}", thresholds.getExpression(), e.getMessage());
            return false;
        }
    }
}