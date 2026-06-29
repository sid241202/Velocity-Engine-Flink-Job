package in.gov.uidai.dp.velocity.engine.functions;

import in.gov.uidai.dp.velocity.engine.aggregation.BucketStateManager;
import in.gov.uidai.dp.velocity.engine.model.*;
import in.gov.uidai.dp.velocity.engine.utils.FieldExtractor;
import in.gov.uidai.dp.velocity.engine.utils.HavingEvaluator;
import in.gov.uidai.dp.velocity.engine.utils.TimeUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.*;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
public class RuleEvaluatorFunction
        extends KeyedBroadcastProcessFunction<String, Keyed<Event, String, String>, VelocityRule, AggregationResult> {

    private static final long serialVersionUID = 2L;

    public static final OutputTag<AnomalyEvent> ANOMALY_TAG = new OutputTag<AnomalyEvent>("anomaly-events") {};
    public static final OutputTag<AnomalyEvent> REDIS_TAG   = new OutputTag<AnomalyEvent>("anomaly-redis") {};

    private transient ObjectMapper mapper;
    private transient BucketStateManager bucketStateManager;
    private transient ValueState<RuleSnapshot> ruleSnapshotState;
    private transient ValueState<Set<String>> anomalyFiredState;

    public RuleEvaluatorFunction() {}

    @Override
    public void open(OpenContext parameters) throws Exception {
        mapper = new ObjectMapper();
        StateTtlConfig ttl = StateTtlConfig.newBuilder(Duration.ofHours(48))
                .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .cleanupInRocksdbCompactFilter(1000).build();
        bucketStateManager = new BucketStateManager(getRuntimeContext(), ttl);
        ValueStateDescriptor<RuleSnapshot> snapDesc = new ValueStateDescriptor<>("rule_snapshot", RuleSnapshot.class);
        snapDesc.enableTimeToLive(ttl);
        ruleSnapshotState = getRuntimeContext().getState(snapDesc);
        ValueStateDescriptor<Set<String>> firedDesc = new ValueStateDescriptor<>("anomaly_fired", (Class<Set<String>>) (Class<?>) HashSet.class);
        firedDesc.enableTimeToLive(ttl);
        anomalyFiredState = getRuntimeContext().getState(firedDesc);
    }

    @Override
    public void processElement(Keyed<Event, String, String> keyedEvent, ReadOnlyContext ctx, Collector<AggregationResult> out) throws Exception {
        VelocityRule rule = ctx.getBroadcastState(DynamicKeyFunction.RULE_STATE_DESC).get(keyedEvent.getId());
        if (rule == null || !rule.isActive()) return;

        RuleSnapshot snap = ruleSnapshotState.value();
        RuleSnapshot newSnap = RuleSnapshot.fromRule(rule);
        if (snap == null || !snap.equals(newSnap)) ruleSnapshotState.update(newSnap);

        long eventTs = resolveEventTs(keyedEvent.getWrapped(), rule, ctx);
        bucketStateManager.addEvent(rule, keyedEvent.getWrapped(), eventTs);

        long slideMs = rule.getWindowing().getEffectiveSlideMs();
        long offsetMs = rule.getWindowing().getEffectiveAlignmentOffsetMs();
        long sizeMs = rule.getWindowing().getSizeMs();
        long latenessMs = rule.getWindowing().getAllowedLatenessMs();

        if (rule.getWindowing().isEventTime()) {
            long nextTimer = TimeUtils.floorToSlide(eventTs, slideMs, offsetMs) + slideMs;
            ctx.timerService().registerEventTimeTimer(nextTimer);
            Long wm = ctx.timerService().currentWatermark();
            if (wm != null && wm != Long.MIN_VALUE) {
                long winEnd = TimeUtils.floorToSlide(eventTs, slideMs, offsetMs) + slideMs;
                if (winEnd <= wm && winEnd + latenessMs > wm) {
                    emitAggLate(rule, winEnd - sizeMs, winEnd, ctx, out);
                }
            }
        } else {
            long now = ctx.timerService().currentProcessingTime();
            ctx.timerService().registerProcessingTimeTimer(TimeUtils.floorToSlide(now, slideMs, offsetMs) + slideMs);
        }

        SinkConfig sinks = rule.getEffectiveSinks();
        if (sinks.isAnomalySinkEnabled() || sinks.isAnomalyStoreSinkEnabled()) {
            tryEarlyFire(keyedEvent.getWrapped(), rule, eventTs, slideMs, offsetMs, ctx);
        }
    }

    private void tryEarlyFire(Event event, VelocityRule rule, long eventTs, long slideMs, long offsetMs, ReadOnlyContext ctx) throws Exception {
        long winStart = TimeUtils.floorToSlide(eventTs, slideMs, offsetMs);
        String bucketKey = rule.getRuleId() + "#" + winStart;
        Set<String> fired = anomalyFiredState.value();
        if (fired != null && fired.contains(bucketKey)) return;

        Map<String, Double> curr = bucketStateManager.computeWindowNoPrune(rule.getAggregations(), winStart, winStart + slideMs);
        curr.remove("_raw_events_");
        if (curr.isEmpty() || !HavingEvaluator.evaluate(rule.getHavingThresholds(), curr)) return;

        String groupKey = getGroupKey(ctx.getCurrentKey(), rule.getRuleId());
        String anomalyVal = resolveAnomalyVal(event, rule, groupKey);
        AnomalyEvent anomaly = new AnomalyEvent(rule.getRuleId(), anomalyVal, TimeUtils.currentIstString(), rule.getPenaltyTtlSeconds());

        SinkConfig sinks = rule.getEffectiveSinks();
        if (sinks.isAnomalySinkEnabled()) ctx.output(ANOMALY_TAG, anomaly);
        if (sinks.isAnomalyStoreSinkEnabled()) ctx.output(REDIS_TAG, anomaly);

        if (fired == null) fired = new HashSet<>();
        fired.add(bucketKey);
        anomalyFiredState.update(fired);
        log.info("[EARLY-FIRE] rule={} groupKey={} entityVal={} winStart={}", rule.getRuleId(), groupKey, anomalyVal, TimeUtils.epochMsToIstString(winStart));
    }

    @Override
    public void processBroadcastElement(VelocityRule rule, Context ctx, Collector<AggregationResult> out) throws Exception {
        if (rule == null) return;
        String ruleId = rule.getRuleId();
        if (ruleId == null || ruleId.isBlank()) { log.warn("Null/blank ruleId — skipping"); return; }
        if (rule.isDeleted()) { ctx.getBroadcastState(DynamicKeyFunction.RULE_STATE_DESC).remove(ruleId); log.info("Removed DELETED rule: {}", ruleId); }
        else { ctx.getBroadcastState(DynamicKeyFunction.RULE_STATE_DESC).put(ruleId, rule); log.info("Updated rule: {} status={}", ruleId, rule.getStatus()); }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<AggregationResult> out) throws Exception {
        RuleSnapshot rule = ruleSnapshotState.value();
        if (rule == null) {
            log.warn("[TIMER] key={} ts={} — ruleSnapshotState is null (state TTL may have expired after checkpoint restore). " +
                     "Window result dropped. Re-broadcast the rule to re-activate.",
                     ctx.getCurrentKey(), timestamp);
            return;
        }

        long winEnd   = timestamp;
        long winStart = winEnd - rule.getWindowing().getSizeMs();
        Map<String, Double> results = bucketStateManager.computeWindowAndPrune(rule.getAggregations(), winStart, winEnd, rule.getAllowedLatenessMs());
        long evtCount = results.containsKey("_raw_events_") ? results.remove("_raw_events_").longValue() : 0L;

        if (evtCount == 0 && results.values().stream().allMatch(v -> v == 0.0)) {
            reRegister(rule, timestamp, ctx); return;
        }

        String groupKey   = getGroupKey(ctx.getCurrentKey(), rule.getRuleId());
        String producedAt = TimeUtils.currentIstString();

        if (rule.getSinks().isAggSinkEnabled()) {
            out.collect(new AggregationResult(
                    rule.getRuleId(), TimeUtils.epochMsToIstString(winStart), TimeUtils.epochMsToIstString(winEnd),
                    rule.getEntityName(), groupKey, serializeMap(results), producedAt));
        }

        boolean breached = HavingEvaluator.evaluate(rule.getHavingThresholds(), results);
        if (breached && (rule.getSinks().isAnomalySinkEnabled() || rule.getSinks().isAnomalyStoreSinkEnabled())) {
            String bucketKey = rule.getRuleId() + "#" + winStart;
            Set<String> fired = anomalyFiredState.value();
            if (fired == null || !fired.contains(bucketKey)) {
                AnomalyEvent anomaly = new AnomalyEvent(rule.getRuleId(), groupKey, producedAt, rule.getPenaltyTtlSeconds());
                if (rule.getSinks().isAnomalySinkEnabled()) ctx.output(ANOMALY_TAG, anomaly);
                if (rule.getSinks().isAnomalyStoreSinkEnabled()) ctx.output(REDIS_TAG, anomaly);
                if (fired == null) fired = new HashSet<>();
                fired.add(bucketKey);
                anomalyFiredState.update(fired);
                log.info("[END-OF-WIN] Anomaly fired rule={} groupKey={}", rule.getRuleId(), groupKey);
            }
        }

        // Prune stale bucketKeys from anomalyFiredState to prevent unbounded growth.
        // Buckets older than (winStart - allowedLatenessMs) can never fire again.
        long pruneBeforeMs = winStart - rule.getAllowedLatenessMs();
        Set<String> firedForPrune = anomalyFiredState.value();
        if (firedForPrune != null && !firedForPrune.isEmpty()) {
            String rulePrefix = rule.getRuleId() + "#";
            boolean pruneChanged = firedForPrune.removeIf(bk -> {
                if (!bk.startsWith(rulePrefix)) return false;
                try {
                    long bkTs = Long.parseLong(bk.substring(rulePrefix.length()));
                    return bkTs < pruneBeforeMs;
                } catch (NumberFormatException ignore) {
                    return false;
                }
            });
            if (pruneChanged) {
                anomalyFiredState.update(firedForPrune.isEmpty() ? null : firedForPrune);
                log.debug("[PRUNE] key={} pruned stale anomalyFired buckets", ctx.getCurrentKey());
            }
        }

        reRegister(rule, timestamp, ctx);
    }

    private void emitAggLate(VelocityRule rule, long winStart, long winEnd, ReadOnlyContext ctx, Collector<AggregationResult> out) throws Exception {
        if (!rule.getEffectiveSinks().isAggSinkEnabled()) return;
        Map<String, Double> results = bucketStateManager.computeWindowNoPrune(rule.getAggregations(), winStart, winEnd);
        results.remove("_raw_events_");
        if (results.isEmpty()) return;
        String groupKey = getGroupKey(ctx.getCurrentKey(), rule.getRuleId());
        out.collect(new AggregationResult(rule.getRuleId(), TimeUtils.epochMsToIstString(winStart),
                TimeUtils.epochMsToIstString(winEnd), rule.getEntityName(), groupKey, serializeMap(results), TimeUtils.currentIstString()));
    }

    private void reRegister(RuleSnapshot rule, long ts, OnTimerContext ctx) throws Exception {
        if (bucketStateManager.isEmpty()) { ruleSnapshotState.clear(); return; }
        long slideMs = rule.getWindowing().getEffectiveSlideMs();
        long offsetMs = rule.getWindowing().getEffectiveAlignmentOffsetMs();
        long next = TimeUtils.floorToSlide(ts, slideMs, offsetMs) + slideMs;
        if (rule.getWindowing().isEventTime()) ctx.timerService().registerEventTimeTimer(next);
        else ctx.timerService().registerProcessingTimeTimer(next);
    }

    private long resolveEventTs(Event event, VelocityRule rule, ReadOnlyContext ctx) {
        long ts = -1L;
        if (rule.getWindowing().isUseKafkaTimestamp()) {
            Object k = event.getFields().get("_kafka_timestamp");
            if (k instanceof Number) ts = ((Number) k).longValue();
        } else {
            Object raw = event.getFields().get(rule.getWindowing().getEffectiveTimestampField());
            if (raw != null) {
                try { ts = "ISO_STRING".equalsIgnoreCase(rule.getWindowing().getTimestampFormat())
                        ? TimeUtils.isoStringToEpochMs(String.valueOf(raw)) : Long.parseLong(String.valueOf(raw));
                } catch (Exception e) { log.warn("TS parse failed '{}' rule={}: {}", raw, rule.getRuleId(), e.getMessage()); }
            }
            if (ts <= 0) { Object fb = event.getFields().get("_event_timestamp_epoch_ms"); if (fb instanceof Number) ts = ((Number) fb).longValue(); }
        }
        if (ts <= 0) ts = ctx.timestamp() != null ? ctx.timestamp() : System.currentTimeMillis();
        return ts;
    }

    private String resolveAnomalyVal(Event event, VelocityRule rule, String groupKey) {
        String field = rule.getAnomalyEntityField();
        if (field != null && !field.isBlank()) {
            Object v = FieldExtractor.extractObject(event, field);
            if (v != null) return String.valueOf(v);
            log.info("anomaly_entity_field '{}' missing — fallback to groupKey", field);
        }
        return groupKey;
    }

    private String getGroupKey(String compositeKey, String ruleId) {
        if (compositeKey != null && compositeKey.startsWith(ruleId + "|")) return compositeKey.substring(ruleId.length() + 1);
        return compositeKey;
    }

    private String serializeMap(Map<String, Double> map) {
        try { if (mapper == null) mapper = new ObjectMapper(); return mapper.writeValueAsString(map); }
        catch (Exception e) { log.error("Map serialize error: {}", e.getMessage()); return "{}"; }
    }
}