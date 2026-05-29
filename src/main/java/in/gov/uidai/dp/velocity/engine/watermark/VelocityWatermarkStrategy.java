package in.gov.uidai.dp.velocity.engine.watermark;

import in.gov.uidai.dp.velocity.engine.model.Event;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import java.time.Duration;

/**
 * Factory for {@link WatermarkStrategy} instances for {@link Event} streams.
 *
 * <h3>Strategy</h3>
 * <ul>
 *   <li>{@code forBoundedOutOfOrderness(allowedLatenessMs)}: the user-configured
 *       out-of-orderness bound. For on-time systems (e.g. auth), set to 0ms.</li>
 *   <li>{@code withIdleness(idlenessMs)}: marks a Kafka partition idle after this
 *       duration with no events, allowing other partitions to advance the watermark.
 *       This prevents idle partitions from blocking the entire watermark.</li>
 * </ul>
 *
 * <p>The timestamp is read from the injected {@code _event_timestamp_epoch_ms} field
 * (Long), which is set by
 * {@link in.gov.uidai.dp.velocity.engine.deserializers.EventDeserializer}.
 *
 * <p>Note: rule evaluation windows are triggered by processing-time timers in
 * {@link in.gov.uidai.dp.velocity.engine.functions.RuleEvaluatorFunction}, NOT
 * by watermarks. Watermarks are only used for event ordering and late-event detection.
 */
@Slf4j
public final class VelocityWatermarkStrategy {

    private VelocityWatermarkStrategy() {}

    /**
     * Create a watermark strategy for an event-time source.
     *
     * @param allowedLatenessMs out-of-orderness bound in ms (user-configured; 0 for on-time topics)
     * @param idlenessMs        idle partition timeout in ms (default 60000)
     */
    public static WatermarkStrategy<Event> forSource(long allowedLatenessMs, long idlenessMs) {
        return WatermarkStrategy
                .<Event>forBoundedOutOfOrderness(Duration.ofMillis(allowedLatenessMs))
                .withIdleness(Duration.ofMillis(idlenessMs))
                .withTimestampAssigner((event, recordTimestamp) -> {
                    Object ts = event.getFields().get("_event_timestamp_epoch_ms");
                    if (ts instanceof Number) {
                        return ((Number) ts).longValue();
                    }
                    log.debug("_event_timestamp_epoch_ms not found in event, using Kafka record timestamp");
                    return recordTimestamp;
                });
    }

    /** No-watermarks strategy for processing-time rules. */
    public static WatermarkStrategy<Event> noTimestamps() {
        return WatermarkStrategy.noWatermarks();
    }
}