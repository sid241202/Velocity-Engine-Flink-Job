package in.gov.uidai.dp.velocity.engine.watermark;

import in.gov.uidai.dp.velocity.engine.model.Event;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import java.time.Duration;

@Slf4j
public final class VelocityWatermarkStrategy {

    private VelocityWatermarkStrategy() {}

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

    public static WatermarkStrategy<Event> noTimestamps() {
        return WatermarkStrategy.noWatermarks();
    }
}