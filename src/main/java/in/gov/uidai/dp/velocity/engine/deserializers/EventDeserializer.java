package in.gov.uidai.dp.velocity.engine.deserializers;

import com.codahale.metrics.SlidingWindowReservoir;
import com.fasterxml.jackson.databind.ObjectMapper;
import in.gov.uidai.dp.velocity.engine.model.Event;
import in.gov.uidai.dp.velocity.engine.utils.TimeUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

@Slf4j
public class EventDeserializer implements KafkaRecordDeserializationSchema<Event> {

    private static final long serialVersionUID = 1L;

    // Sliding window (not a decaying/exponential reservoir): these are ops
    // dashboards for "what's happening right now", so a plain recent-N-samples
    // window is more legible than time-decayed weighting.
    private static final int HISTOGRAM_RESERVOIR_SIZE = 500;

    private final String sourceTopic;
    private final String eventTimestampField;
    private final String eventTimestampFormat;

    private transient ObjectMapper objectMapper;

    // Registered once in open(), reused for every record — never recreated
    // per-record.
    private transient Counter eventsConsumedCounter;
    private transient Counter emptyRecordCounter;
    private transient Counter parseErrorCounter;
    private transient Histogram ingestDelayMsHistogram;

    public EventDeserializer(String sourceTopic,
            String eventTimestampField, String eventTimestampFormat) {
        this.sourceTopic = sourceTopic;
        this.eventTimestampField = eventTimestampField;
        this.eventTimestampFormat = eventTimestampFormat;
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        MetricGroup topicGroup = context.getMetricGroup().addGroup("topic", sourceTopic);
        eventsConsumedCounter = topicGroup.counter("velocity_events_consumed_total");
        emptyRecordCounter = topicGroup.addGroup("error_type", "empty_record")
                .counter("velocity_deserialization_errors_total");
        parseErrorCounter = topicGroup.addGroup("error_type", "parse_error")
                .counter("velocity_deserialization_errors_total");
        // Named _ms, not _seconds: Flink's Histogram.update() only accepts a
        // long, and these delays are routinely sub-second — truncating to
        // whole seconds would lose virtually all resolution.
        ingestDelayMsHistogram = topicGroup.histogram("velocity_ingest_delay_ms",
                new DropwizardHistogramWrapper(new com.codahale.metrics.Histogram(
                        new SlidingWindowReservoir(HISTOGRAM_RESERVOIR_SIZE))));
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<Event> out) throws IOException {
        if (record.value() == null || record.value().length == 0) {
            log.warn("Received null/empty record from topic={} partition={} offset={}",
                    record.topic(), record.partition(), record.offset());
            emptyRecordCounter.inc();
            return;
        }

        try {
            ensureObjectMapper();

            Event event = objectMapper.readValue(record.value(), Event.class);

            event.put("_source_topic", sourceTopic);
            event.put("_kafka_partition", record.partition());
            event.put("_kafka_offset", record.offset());
            event.put("_kafka_timestamp", record.timestamp());

            long epochMs = extractEpochMs(event, record.timestamp());
            event.put("_event_timestamp_epoch_ms", epochMs);

            Object dataObj = event.getFields().get("_data");
            if (dataObj != null && !(dataObj instanceof String)) {
                event.put("_data", objectMapper.writeValueAsString(dataObj));
            }

            eventsConsumedCounter.inc();
            long ingestDelayMs = System.currentTimeMillis() - epochMs;
            ingestDelayMsHistogram.update(Math.max(0L, ingestDelayMs));

            out.collect(event);

        } catch (Exception e) {
            log.error("Failed to deserialize event from topic={} offset={}: {}",
                    sourceTopic, record.offset(), e.getMessage());
            parseErrorCounter.inc();
        }
    }

    private long extractEpochMs(Event event, long kafkaTimestamp) {
        Object rawTs = event.getFields().get(eventTimestampField);
        if (rawTs == null) {
            log.info("Timestamp field '{}' not found in event, using Kafka timestamp", eventTimestampField);
            return kafkaTimestamp;
        }

        try {
            if ("EPOCH_MILLIS".equalsIgnoreCase(eventTimestampFormat)) {
                if (rawTs instanceof Number) {
                    return ((Number) rawTs).longValue();
                }
                return Long.parseLong(rawTs.toString().trim());
            } else {
                long parsed = TimeUtils.isoStringToEpochMs(rawTs.toString().trim());
                return (parsed == -1) ? kafkaTimestamp : parsed;
            }
        } catch (Exception e) {
            log.warn("Could not parse timestamp '{}' from field '{}', using Kafka timestamp: {}",
                    rawTs, eventTimestampField, e.getMessage());
            return kafkaTimestamp;
        }
    }

    @Override
    public TypeInformation<Event> getProducedType() {
        return TypeInformation.of(Event.class);
    }

    private void ensureObjectMapper() {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
            objectMapper.configure(
                    com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,
                    false);
        }
    }
}