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

@Slf4j
public class RuleEvaluatorFunction
        extends KeyedBroadcastProcessFunction<String, Keyed<Event, String, String>, VelocityRule, AggregationResult> {

    private static final long serialVersionUID = 1L;

    public static final OutputTag<VelocityAlert> ALERT_TAG = new OutputTag<VelocityAlert>("velocity-alerts") {
    };

    private final String cluster;
    private transient ObjectMapper mapper;

    private transient BucketStateManager bucketStateManager;

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
            return;
        }

        RuleSnapshot snapshot = ruleSnapshotState.value();
        RuleSnapshot newSnapshot = RuleSnapshot.fromRule(rule, cluster);
        if (snapshot == null || !snapshot.equals(newSnapshot)) {
            ruleSnapshotState.update(newSnapshot);
        }

        long eventTs = -1L;
        if (rule.getWindowing().isUseKafkaTimestamp()) {
            Object kTs = keyedEvent.getWrapped().getFields().get("_kafka_timestamp");
            if (kTs != null) {
                eventTs = ((Number) kTs).longValue();
            }
        } else {
            Object rawTs = keyedEvent.getWrapped().getFields().get(rule.getWindowing().getEffectiveTimestampField());
            if (rawTs != null) {
                try {
                    if ("ISO_STRING".equalsIgnoreCase(rule.getWindowing().getTimestampFormat())) {
                        eventTs = TimeUtils.isoStringToEpochMs(String.valueOf(rawTs));
                    } else {
                        eventTs = Long.parseLong(String.valueOf(rawTs));
                    }
                } catch (Exception ignored) {
                }
            }
            if (eventTs <= 0) {
                Object fallbackTs = keyedEvent.getWrapped().getFields().get("_event_timestamp_epoch_ms");
                if (fallbackTs != null) {
                    eventTs = ((Number) fallbackTs).longValue();
                }
            }
        }
        if (eventTs <= 0) {
            eventTs = ctx.timestamp() != null ? ctx.timestamp() : System.currentTimeMillis();
        }

        bucketStateManager.addEvent(rule, keyedEvent.getWrapped(), eventTs);

        long slideMs = rule.getWindowing().getEffectiveSlideMs();
        long offsetMs = rule.getWindowing().getEffectiveAlignmentOffsetMs();
        long sizeMs = rule.getWindowing().getSizeMs();
        long allowedLatenessMs = rule.getWindowing().getAllowedLatenessMs();

        if (rule.getWindowing().isEventTime()) {
            long nextTimer = TimeUtils.floorToSlide(eventTs, slideMs, offsetMs) + slideMs;
            ctx.timerService().registerEventTimeTimer(nextTimer);

            // Late event detection (Option A: On-Demand)
            // If this event belongs to a window that has already closed, emit an updated result NOW
            Long currentWatermark = ctx.timerService().currentWatermark();
            if (currentWatermark != null && currentWatermark != Long.MIN_VALUE) {
                long eventWindowEnd = TimeUtils.floorToSlide(eventTs, slideMs, offsetMs) + slideMs;
                if (eventWindowEnd <= currentWatermark && eventWindowEnd + allowedLatenessMs > currentWatermark) {
                    // This event is late but within the allowed lateness — emit updated result immediately
                    long lateWindowStart = eventWindowEnd - sizeMs;
                    emitResult(rule, lateWindowStart, eventWindowEnd, allowedLatenessMs, ctx, out);
                }
            }
        } else {
            long currentProcessingTime = ctx.timerService().currentProcessingTime();
            long nextTimer = TimeUtils.floorToSlide(currentProcessingTime, slideMs, offsetMs) + slideMs;
            ctx.timerService().registerProcessingTimeTimer(nextTimer);
        }
    }


    @Override
    public void processBroadcastElement(VelocityRule rule, Context ctx, Collector<AggregationResult> out)
            throws Exception {
        if (rule == null) return;

        String ruleId = rule.getRuleId();
        if (ruleId == null || ruleId.isBlank()) {
            log.warn("Received rule with null/blank ruleId — skipping");
            return;
        }

        if (rule.isDeleted()) {
            log.info("Removing DELETED rule from broadcast state: id={}", ruleId);
            ctx.getBroadcastState(DynamicKeyFunction.RULE_STATE_DESC).remove(ruleId);
        } else {
            // Store ACTIVE and PAUSED rules. processElement() checks isActive()
            // so PAUSED rules will be skipped during evaluation but remain in state
            // for instant resumption.
            log.info("Updating rule in broadcast state: id={} status={}", ruleId, rule.getStatus());
            ctx.getBroadcastState(DynamicKeyFunction.RULE_STATE_DESC).put(ruleId, rule);
        }
    }



    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<AggregationResult> out) throws Exception {
        RuleSnapshot rule = ruleSnapshotState.value();
        if (rule == null) {
            return;
        }

        long windowEndTs = timestamp;
        long windowStartTs = windowEndTs - rule.getWindowing().getSizeMs();
        long allowedLatenessMs = rule.getAllowedLatenessMs();

        VelocityRule mockRule = new VelocityRule();
        mockRule.setAggregations(rule.getAggregations());

        Map<String, Double> results = bucketStateManager.computeWindowAndPrune(mockRule, windowStartTs, windowEndTs, allowedLatenessMs);

        long windowEventCount = 0L;
        if (results.containsKey("_raw_events_")) {
            windowEventCount = results.remove("_raw_events_").longValue();
        }

        // Skip emitting if window has zero events (can happen when only old buckets were pruned)
        if (windowEventCount == 0 && results.values().stream().allMatch(v -> v == 0.0)) {
            reRegisterTimerIfNeeded(rule, timestamp, ctx);
            return;
        }

        boolean breached = HavingEvaluator.evaluate(rule.getHavingThresholds(), results);

        String ruleId = rule.getRuleId();
        String groupKey = getGroupKey(ctx.getCurrentKey(), ruleId);

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
                results,
                breached ? 1 : 0,
                rule.getSeverityLevel(),
                windowEventCount,
                TimeUtils.currentIstString());
        out.collect(result);

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
                    mapper.writeValueAsString(results),
                    result.getEvaluatedAt());
            ctx.output(ALERT_TAG, alert);
        }

        reRegisterTimerIfNeeded(rule, timestamp, ctx);
    }

    /**
     * Re-registers the next slide timer if there is still state, otherwise clears the snapshot.
     */
    private void reRegisterTimerIfNeeded(RuleSnapshot rule, long timestamp, OnTimerContext ctx) throws Exception {
        if (bucketStateManager.isEmpty()) {
            ruleSnapshotState.clear();
            log.debug("State is empty for key {}, clearing snapshot and stopping timers.", ctx.getCurrentKey());
            return;
        }

        long slideMs = rule.getWindowing().getEffectiveSlideMs();
        long offsetMs = rule.getWindowing().getEffectiveAlignmentOffsetMs();
        long nextTimer = TimeUtils.floorToSlide(timestamp, slideMs, offsetMs) + slideMs;
        if (rule.getWindowing().isEventTime()) {
            ctx.timerService().registerEventTimeTimer(nextTimer);
        } else {
            ctx.timerService().registerProcessingTimeTimer(nextTimer);
        }
    }

    /**
     * Emits an updated AggregationResult for a late event without pruning state.
     * Used for on-demand re-evaluation when a late event arrives for an already-closed window.
     */
    private void emitResult(VelocityRule rule, long windowStartTs, long windowEndTs, long allowedLatenessMs,
                            ReadOnlyContext ctx, Collector<AggregationResult> out) throws Exception {
        VelocityRule mockRule = new VelocityRule();
        mockRule.setAggregations(rule.getAggregations());

        // Use computeWindowNoPrune — do NOT evict state for late re-evaluations
        Map<String, Double> results = bucketStateManager.computeWindowNoPrune(mockRule, windowStartTs, windowEndTs);

        long windowEventCount = 0L;
        if (results.containsKey("_raw_events_")) {
            windowEventCount = results.remove("_raw_events_").longValue();
        }
        if (windowEventCount == 0) return;

        boolean breached = HavingEvaluator.evaluate(rule.getHavingThresholds(), results);

        String ruleId = rule.getRuleId();
        String groupKey = getGroupKey(ctx.getCurrentKey(), ruleId);

        AggregationResult result = new AggregationResult(
                ruleId,
                rule.getRuleName(),
                rule.getSourceTopic(),
                rule.getSourceCluster(),
                groupKey,
                TimeUtils.epochMsToIstString(windowStartTs),
                TimeUtils.epochMsToIstString(windowEndTs),
                rule.getWindowing().getType(),
                rule.getWindowing().getTimeType(),
                results,
                breached ? 1 : 0,
                rule.getSeverityLevel(),
                windowEventCount,
                TimeUtils.currentIstString());
        out.collect(result);

        if (breached) {
            VelocityAlert alert = new VelocityAlert(
                    ruleId,
                    rule.getRuleName(),
                    rule.getSeverityLevel(),
                    rule.getPenaltyTtlSeconds(),
                    groupKey,
                    rule.getSourceTopic(),
                    rule.getSourceCluster(),
                    result.getWindowStart(),
                    result.getWindowEnd(),
                    mapper.writeValueAsString(results),
                    result.getEvaluatedAt());
            ctx.output(ALERT_TAG, alert);
        }
    }

    private String getGroupKey(String compositeKey, String ruleId) {

        if (compositeKey != null && compositeKey.startsWith(ruleId + "|")) {
            return compositeKey.substring(ruleId.length() + 1);
        }
        return compositeKey;
    }
}