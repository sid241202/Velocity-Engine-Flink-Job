package in.gov.uidai.dp.velocity.engine.functions;

import in.gov.uidai.dp.velocity.engine.aggregation.BucketStateManager;
import in.gov.uidai.dp.velocity.engine.model.*;
import in.gov.uidai.dp.velocity.engine.utils.HavingEvaluator;
import in.gov.uidai.dp.velocity.engine.utils.TimeUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.*;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Map;

/**
 * Core windowing and aggregation logic.
 * Keyed by {@code (RuleID + GroupKey)}.
 *
 * <p>
 * Uses processing-time timers to periodically evaluate rule windows based on
 * {@link WindowingConfig#slideMs}. Bypasses native Flink Window operators
 * to prevent watermark-related stalls on high-cardinality idle keys.
 */
@Slf4j
public class RuleEvaluatorFunction
        extends KeyedBroadcastProcessFunction<String, Keyed<Event, String, String>, VelocityRule, AggregationResult> {

    private static final long serialVersionUID = 1L;

    public static final OutputTag<VelocityAlert> ALERT_TAG = new OutputTag<VelocityAlert>("velocity-alerts") {
    };

    private final String cluster;
    private transient ObjectMapper mapper;

    // --- State ---
    private transient BucketStateManager bucketStateManager;

    // Stores the compact rule snapshot so timers (which can't read broadcast state)
    // know what to evaluate.
    private transient ValueState<RuleSnapshot> ruleSnapshotState;

    public RuleEvaluatorFunction(String cluster) {
        this.cluster = cluster;
    }

    @Override
    public void open(OpenContext parameters) throws Exception {
        mapper = new ObjectMapper();
        bucketStateManager = new BucketStateManager(getRuntimeContext());

        ValueStateDescriptor<RuleSnapshot> ruleSnapshotDesc = new ValueStateDescriptor<>(
                "rule_snapshot", RuleSnapshot.class);
        ruleSnapshotState = getRuntimeContext().getState(ruleSnapshotDesc);
    }

    @Override
    public void processElement(Keyed<Event, String, String> keyedEvent, ReadOnlyContext ctx,
            Collector<AggregationResult> out) throws Exception {
        VelocityRule rule = ctx.getBroadcastState(DynamicKeyFunction.RULE_STATE_DESC).get(keyedEvent.getId());

        if (rule == null || !rule.isActive()) {
            return; // Rule was deleted or paused since DynamicKeyFunction passed it
        }

        // 1. Update rule snapshot for timers
        RuleSnapshot snapshot = ruleSnapshotState.value();
        if (snapshot == null || !snapshot.getRuleName().equals(rule.getRuleName())) {
            ruleSnapshotState.update(RuleSnapshot.fromRule(rule, cluster));
        }

        // 2. Add event to bucket state
        long eventTs = -1L;
        Object rawTs = keyedEvent.getWrapped().getFields().get(rule.getWindowing().getEffectiveTimestampField());
        if (rawTs != null) {
            try {
                if ("ISO_STRING".equalsIgnoreCase(rule.getWindowing().getTimestampFormat())) {
                    eventTs = TimeUtils.isoStringToEpochMs(String.valueOf(rawTs));
                } else {
                    eventTs = Long.parseLong(String.valueOf(rawTs));
                }
            } catch (Exception ignored) {}
        }
        if (eventTs <= 0) {
            Object fallbackTs = keyedEvent.getWrapped().getFields().get("_event_timestamp_epoch_ms");
            if (fallbackTs != null) {
                eventTs = (Long) fallbackTs;
            }
        }
        
        bucketStateManager.addEvent(rule, keyedEvent.getWrapped(), eventTs);

        // 3. Register timer for the next slide evaluation
        long currentProcessingTime = ctx.timerService().currentProcessingTime();
        long slideMs = rule.getWindowing().getEffectiveSlideMs();
        long nextTimer = TimeUtils.floorToSlide(currentProcessingTime, slideMs) + slideMs;
        ctx.timerService().registerProcessingTimeTimer(nextTimer);
    }

    @Override
    public void processBroadcastElement(VelocityRule rule, Context ctx, Collector<AggregationResult> out)
            throws Exception {
        // Broadcast state is automatically handled by Flink via RULE_STATE_DESC map.
        // The read-only version is accessed in processElement.
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<AggregationResult> out) throws Exception {
        RuleSnapshot rule = ruleSnapshotState.value();
        if (rule == null) {
            // State might have been cleared or rule deleted, ignore timer
            return;
        }

        // Timer fired at exactly `timestamp`. The evaluation covers the window ending
        // at `timestamp`.
        long windowEndTs = timestamp;
        long windowStartTs = windowEndTs - rule.getWindowing().getSizeMs();

        // 1. Compute aggregations and prune old state
        // Re-construct a mock VelocityRule to pass to bucket manager (since it requires
        // AggregationSpec)
        VelocityRule mockRule = new VelocityRule();
        mockRule.setAggregations(rule.getAggregations());

        Map<String, Double> results = bucketStateManager.computeWindowAndPrune(mockRule, windowStartTs, windowEndTs);

        // If no events fell in this window (all counts 0), we could optionally skip
        // emission.
        // For now, emit 0-state windows to ClickHouse for continuity in charts.

        // 2. Evaluate Having Thresholds
        boolean breached = HavingEvaluator.evaluate(rule.getHavingThresholds(), results);

        // 3. Construct ClickHouse Result
        String ruleId = rule.getRuleId();
        String groupKey = getGroupKey(ctx.getCurrentKey(), ruleId); // key is ruleId + "|" + groupKey
        String resultJson = mapper.writeValueAsString(results);

        AggregationResult result = new AggregationResult(
                ruleId,
                rule.getRuleName(),
                rule.getSourceTopic(),
                rule.getCluster(),
                groupKey,
                TimeUtils.epochMsToIstString(windowStartTs),
                TimeUtils.epochMsToIstString(windowEndTs),
                rule.getWindowing().getType(),
                rule.getWindowing().getTimeType(),
                resultJson,
                breached ? 1 : 0,
                rule.getSeverityLevel(),
                0L, // event count approximation could be added to bucket state if needed
                TimeUtils.currentIstString());
        out.collect(result);

        // 4. Emit Alert to KeyDB Side Output if breached
        if (breached) {
            VelocityAlert alert = new VelocityAlert(
                    ruleId,
                    rule.getRuleName(),
                    rule.getSeverityLevel(),
                    rule.getPenaltyTtlSeconds(),
                    groupKey,
                    rule.getSourceTopic(),
                    rule.getCluster(),
                    result.getWindowStart(),
                    result.getWindowEnd(),
                    resultJson,
                    result.getEvaluatedAt());
            ctx.output(ALERT_TAG, alert);
        }

        // 5. Register next timer (keep the loop going as long as state exists)
        // Note: In a production system, we'd add logic to stop registering timers if
        // state is totally empty
        // to prevent infinite timers on dead keys. For now, we continue polling.
        long nextTimer = timestamp + rule.getWindowing().getEffectiveSlideMs();
        ctx.timerService().registerProcessingTimeTimer(nextTimer);
    }

    private String getGroupKey(String compositeKey, String ruleId) {
        // compositeKey = ruleId + "|" + groupKey (from keyBy function in pipeline)
        if (compositeKey != null && compositeKey.startsWith(ruleId + "|")) {
            return compositeKey.substring(ruleId.length() + 1);
        }
        return compositeKey;
    }
}