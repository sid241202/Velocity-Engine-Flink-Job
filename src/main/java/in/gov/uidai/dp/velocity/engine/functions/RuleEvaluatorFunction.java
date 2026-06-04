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
        if (snapshot == null || !snapshot.getRuleName().equals(rule.getRuleName())) {
            ruleSnapshotState.update(RuleSnapshot.fromRule(rule, cluster));
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
        
        if (rule.getWindowing().isEventTime()) {
            long nextTimer = TimeUtils.floorToSlide(eventTs, slideMs, offsetMs) + slideMs;
            ctx.timerService().registerEventTimeTimer(nextTimer);
        } else {
            long currentProcessingTime = ctx.timerService().currentProcessingTime();
            long nextTimer = TimeUtils.floorToSlide(currentProcessingTime, slideMs, offsetMs) + slideMs;
            ctx.timerService().registerProcessingTimeTimer(nextTimer);
        }
    }

    @Override
    public void processBroadcastElement(VelocityRule rule, Context ctx, Collector<AggregationResult> out)
            throws Exception {

    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<AggregationResult> out) throws Exception {
        RuleSnapshot rule = ruleSnapshotState.value();
        if (rule == null) {
            return;
        }

        long windowEndTs = timestamp;
        long windowStartTs = windowEndTs - rule.getWindowing().getSizeMs();

        VelocityRule mockRule = new VelocityRule();
        mockRule.setAggregations(rule.getAggregations());

        Map<String, Double> results = bucketStateManager.computeWindowAndPrune(mockRule, windowStartTs, windowEndTs);

        long windowEventCount = 0L;
        if (results.containsKey("_raw_events_")) {
            windowEventCount = results.remove("_raw_events_").longValue();
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

        if (bucketStateManager.isEmpty()) {
            ruleSnapshotState.clear();
            log.debug("State is empty for key {}, clearing snapshot and stopping timers.", ctx.getCurrentKey());
            return;
        }

        long offsetMs = rule.getWindowing().getEffectiveAlignmentOffsetMs();
        long nextTimer = TimeUtils.floorToSlide(timestamp, rule.getWindowing().getEffectiveSlideMs(), offsetMs) + rule.getWindowing().getEffectiveSlideMs();
        if (rule.getWindowing().isEventTime()) {
            ctx.timerService().registerEventTimeTimer(nextTimer);
        } else {
            ctx.timerService().registerProcessingTimeTimer(nextTimer);
        }
    }

    private String getGroupKey(String compositeKey, String ruleId) {

        if (compositeKey != null && compositeKey.startsWith(ruleId + "|")) {
            return compositeKey.substring(ruleId.length() + 1);
        }
        return compositeKey;
    }
}