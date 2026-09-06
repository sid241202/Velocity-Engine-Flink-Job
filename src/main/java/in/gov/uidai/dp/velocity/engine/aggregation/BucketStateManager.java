package in.gov.uidai.dp.velocity.engine.aggregation;

import in.gov.uidai.dp.velocity.engine.model.AggregationSpec;
import in.gov.uidai.dp.velocity.engine.model.Event;
import in.gov.uidai.dp.velocity.engine.model.VelocityRule;
import in.gov.uidai.dp.velocity.engine.utils.FieldExtractor;
import in.gov.uidai.dp.velocity.engine.utils.TimeUtils;
import in.gov.uidai.dp.velocity.engine.aggregation.functions.*;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.StateTtlConfig;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BucketStateManager {

    private final CountAccumulator countAcc;
    private final SumAccumulator sumAcc;
    private final AvgAccumulator avgAcc;
    private final MinAccumulator minAcc;
    private final MaxAccumulator maxAcc;
    private final CountDistinctExact distinctExactAcc;
    private final CountDistinctHll distinctHllAcc;
    private final CountAccumulator rawEventCountAcc;

    public BucketStateManager(RuntimeContext ctx, StateTtlConfig ttlConfig) {
        this.countAcc = new CountAccumulator(ctx, ttlConfig);
        this.sumAcc = new SumAccumulator(ctx, ttlConfig);
        this.avgAcc = new AvgAccumulator(ctx, ttlConfig);
        this.minAcc = new MinAccumulator(ctx, ttlConfig);
        this.maxAcc = new MaxAccumulator(ctx, ttlConfig);
        this.distinctExactAcc = new CountDistinctExact(ctx, ttlConfig);
        this.distinctHllAcc = new CountDistinctHll(ctx, ttlConfig);
        this.rawEventCountAcc = new CountAccumulator(ctx, "raw_event_count_acc", ttlConfig);
    }

    public void addEvent(VelocityRule rule, Event event, long eventTs) throws Exception {
        long bucketTs = TimeUtils.floorToSlide(eventTs, rule.getWindowing().getEffectiveSlideMs(), rule.getWindowing().getEffectiveAlignmentOffsetMs());

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

    public Map<String, Double> computeWindowAndPrune(List<AggregationSpec> aggregations, long windowStartTs, long windowEndTs, long allowedLatenessMs) throws Exception {
        Map<String, Double> results = new HashMap<>();

        for (AggregationSpec spec : aggregations) {
            double finalVal = 0.0;
            String alias = spec.getAlias();

            if (spec.isCount()) {
                finalVal = countAcc.computeAndPrune(alias, windowStartTs, windowEndTs, allowedLatenessMs);
            } else if (spec.isSum()) {
                finalVal = sumAcc.computeAndPrune(alias, windowStartTs, windowEndTs, allowedLatenessMs);
            } else if (spec.isAvg()) {
                finalVal = avgAcc.computeAndPrune(alias, windowStartTs, windowEndTs, allowedLatenessMs);
            } else if (spec.isMin()) {
                finalVal = minAcc.computeAndPrune(alias, windowStartTs, windowEndTs, allowedLatenessMs);
            } else if (spec.isMax()) {
                finalVal = maxAcc.computeAndPrune(alias, windowStartTs, windowEndTs, allowedLatenessMs);
            } else if (spec.isCountDistinct()) {
                if (spec.isHighCardinality()) {
                    finalVal = distinctHllAcc.computeAndPrune(alias, windowStartTs, windowEndTs, allowedLatenessMs);
                } else {
                    finalVal = distinctExactAcc.computeAndPrune(alias, windowStartTs, windowEndTs, allowedLatenessMs);
                }
            }
            results.put(alias, finalVal);
        }

        double rawCount = rawEventCountAcc.computeAndPrune("_raw_events_", windowStartTs, windowEndTs, allowedLatenessMs);
        results.put("_raw_events_", rawCount);

        return results;
    }

    public Map<String, Double> computeWindowNoPrune(List<AggregationSpec> aggregations, long windowStartTs, long windowEndTs) throws Exception {
        Map<String, Double> results = new HashMap<>();

        for (AggregationSpec spec : aggregations) {
            double finalVal = 0.0;
            String alias = spec.getAlias();

            if (spec.isCount()) {
                finalVal = countAcc.computeNoPrune(alias, windowStartTs, windowEndTs);
            } else if (spec.isSum()) {
                finalVal = sumAcc.computeNoPrune(alias, windowStartTs, windowEndTs);
            } else if (spec.isAvg()) {
                finalVal = avgAcc.computeNoPrune(alias, windowStartTs, windowEndTs);
            } else if (spec.isMin()) {
                finalVal = minAcc.computeNoPrune(alias, windowStartTs, windowEndTs);
            } else if (spec.isMax()) {
                finalVal = maxAcc.computeNoPrune(alias, windowStartTs, windowEndTs);
            } else if (spec.isCountDistinct()) {
                if (spec.isHighCardinality()) {
                    finalVal = distinctHllAcc.computeNoPrune(alias, windowStartTs, windowEndTs);
                } else {
                    finalVal = distinctExactAcc.computeNoPrune(alias, windowStartTs, windowEndTs);
                }
            }
            results.put(alias, finalVal);
        }

        double rawCount = rawEventCountAcc.computeNoPrune("_raw_events_", windowStartTs, windowEndTs);
        results.put("_raw_events_", rawCount);

        return results;
    }

    /**
     * Approximate total bucket-state key count, for the velocity_window_state_keys
     * gauge. Uses rawEventCountAcc alone rather than summing across all 8
     * accumulators: addEvent() unconditionally adds to rawEventCountAcc for
     * every windowed rule regardless of which aggregation functions it uses,
     * so its key count already tracks (rule, bucket) growth across the whole
     * windowed-rule population — the accumulators for specific aggregation
     * types (sum/avg/min/max/distinct) grow and shrink in lockstep with it,
     * not independently, so summing all 8 would just multiply the same signal
     * without adding information.
     */
    public long rawEventKeyCount() throws Exception {
        return rawEventCountAcc.keyCount();
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