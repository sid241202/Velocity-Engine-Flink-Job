package in.gov.uidai.dp.velocity.engine.aggregation;

import in.gov.uidai.dp.velocity.engine.model.AggregationSpec;
import in.gov.uidai.dp.velocity.engine.model.Event;
import in.gov.uidai.dp.velocity.engine.model.VelocityRule;
import in.gov.uidai.dp.velocity.engine.utils.FieldExtractor;
import in.gov.uidai.dp.velocity.engine.utils.TimeUtils;
import in.gov.uidai.dp.velocity.engine.aggregation.functions.*;
import org.apache.flink.api.common.functions.RuntimeContext;

import java.util.HashMap;
import java.util.Map;

/**
 * Manages the dispatch of events to the correct state accumulators and
 * computes final aggregated values across all unexpired buckets for a window.
 */
public class BucketStateManager {

    private final CountAccumulator countAcc;
    private final SumAccumulator sumAcc;
    private final AvgAccumulator avgAcc;
    private final MinAccumulator minAcc;
    private final MaxAccumulator maxAcc;
    private final CountDistinctExact distinctExactAcc;
    private final CountDistinctHll distinctHllAcc;
    
    // Dedicated accumulator for tracking raw event counts natively
    private final CountAccumulator rawEventCountAcc;

    public BucketStateManager(RuntimeContext ctx) {
        this.countAcc = new CountAccumulator(ctx);
        this.sumAcc = new SumAccumulator(ctx);
        this.avgAcc = new AvgAccumulator(ctx);
        this.minAcc = new MinAccumulator(ctx);
        this.maxAcc = new MaxAccumulator(ctx);
        this.distinctExactAcc = new CountDistinctExact(ctx);
        this.distinctHllAcc = new CountDistinctHll(ctx);
        this.rawEventCountAcc = new CountAccumulator(ctx);
    }

    /**
     * Dispatch an event to the appropriate accumulators for its time bucket.
     */
    public void addEvent(VelocityRule rule, Event event, long eventTs) throws Exception {
        long bucketTs = TimeUtils.floorToSlide(eventTs, rule.getWindowing().getEffectiveSlideMs());

        // Always silently track total raw events in this bucket
        String rawBucketKey = TimeUtils.bucketKey("_raw_events_", bucketTs);
        rawEventCountAcc.add(rawBucketKey);

        for (AggregationSpec spec : rule.getAggregations()) {
            String bucketKey = TimeUtils.bucketKey(spec.getAlias(), bucketTs);
            Object value = FieldExtractor.extractObject(event, spec.getField());

            if (spec.isCount()) {
                countAcc.add(bucketKey);
            } else if (spec.isSum()) {
                Double num = asDouble(value);
                if (num != null) sumAcc.add(bucketKey, num);
            } else if (spec.isAvg()) {
                Double num = asDouble(value);
                if (num != null) avgAcc.add(bucketKey, num);
            } else if (spec.isMin()) {
                Double num = asDouble(value);
                if (num != null) minAcc.add(bucketKey, num);
            } else if (spec.isMax()) {
                Double num = asDouble(value);
                if (num != null) maxAcc.add(bucketKey, num);
            } else if (spec.isCountDistinct()) {
                String strVal = (value != null) ? String.valueOf(value) : null;
                if (strVal != null) {
                    if (spec.isHighCardinality()) {
                        distinctHllAcc.add(bucketKey, strVal);
                    } else {
                        distinctExactAcc.add(bucketKey, strVal);
                    }
                }
            }
        }
    }

    /**
     * Calculate final aggregation results for all buckets that fall within
     * the window [windowStartTs, windowEndTs). Expired buckets are pruned.
     */
    public Map<String, Double> computeWindowAndPrune(VelocityRule rule, long windowStartTs, long windowEndTs) throws Exception {
        Map<String, Double> results = new HashMap<>();

        for (AggregationSpec spec : rule.getAggregations()) {
            double finalVal = 0.0;
            String alias = spec.getAlias();

            if (spec.isCount()) {
                finalVal = countAcc.computeAndPrune(alias, windowStartTs, windowEndTs);
            } else if (spec.isSum()) {
                finalVal = sumAcc.computeAndPrune(alias, windowStartTs, windowEndTs);
            } else if (spec.isAvg()) {
                finalVal = avgAcc.computeAndPrune(alias, windowStartTs, windowEndTs);
            } else if (spec.isMin()) {
                finalVal = minAcc.computeAndPrune(alias, windowStartTs, windowEndTs);
            } else if (spec.isMax()) {
                finalVal = maxAcc.computeAndPrune(alias, windowStartTs, windowEndTs);
            } else if (spec.isCountDistinct()) {
                if (spec.isHighCardinality()) {
                    finalVal = distinctHllAcc.computeAndPrune(alias, windowStartTs, windowEndTs);
                } else {
                    finalVal = distinctExactAcc.computeAndPrune(alias, windowStartTs, windowEndTs);
                }
            }
            results.put(alias, finalVal);
        }

        // Add raw event count to results mapping so the Evaluator can extract it easily
        double rawCount = rawEventCountAcc.computeAndPrune("_raw_events_", windowStartTs, windowEndTs);
        results.put("_raw_events_", rawCount);

        return results;
    }

    public boolean isEmpty() throws Exception {
        return countAcc.isEmpty() && sumAcc.isEmpty() && avgAcc.isEmpty() && 
               minAcc.isEmpty() && maxAcc.isEmpty() && distinctExactAcc.isEmpty() && 
               distinctHllAcc.isEmpty() && rawEventCountAcc.isEmpty();
    }

    private Double asDouble(Object val) {
        if (val == null) return null;
        if (val instanceof Number) return ((Number) val).doubleValue();
        try { return Double.parseDouble(String.valueOf(val)); }
        catch (NumberFormatException e) { return null; }
    }
}