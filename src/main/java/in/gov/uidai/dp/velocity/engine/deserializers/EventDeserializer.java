package in.gov.uidai.dp.velocity.engine.deserializers;

import com.fasterxml.jackson.databind.ObjectMapper;
import in.gov.uidai.dp.velocity.engine.model.Event;
import in.gov.uidai.dp.velocity.engine.utils.TimeUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Deserializes Kafka records into {@link Event} objects.
 *
 * <p>Key responsibilities beyond basic JSON deserialization:
 * <ul>
 *   <li>Injects {@code _source_topic}: the Kafka topic name — used for rule routing in
 *       {@link in.gov.uidai.dp.velocity.engine.functions.DynamicKeyFunction}.</li>
 *   <li>Injects {@code _cluster}: the cluster identifier from job config.</li>
 *   <li>Injects {@code _event_timestamp_epoch_ms} (Long): converts the configured timestamp
 *       field (ISO string or epoch millis) to epoch millis for watermark assignment.</li>
 *   <li>Keeps {@code _data} as a raw JSON String — parsed lazily on first field access
 *       via {@link in.gov.uidai.dp.velocity.engine.utils.FieldExtractor} to avoid
 *       deserializing 130+ fields for events that get filtered out immediately.</li>
 * </ul>
 */
@Slf4j
public class EventDeserializer implements KafkaRecordDeserializationSchema<Event> {

    private static final long serialVersionUID = 1L;

    private final String sourceTopic;
    private final String cluster;
    private final String eventTimestampField;
    private final String eventTimestampFormat;  // ISO_STRING | EPOCH_MILLIS

    private transient ObjectMapper objectMapper;

    public EventDeserializer(String sourceTopic, String cluster,
                             String eventTimestampField, String eventTimestampFormat) {
        this.sourceTopic = sourceTopic;
        this.cluster = cluster;
        this.eventTimestampField = eventTimestampField;
        this.eventTimestampFormat = eventTimestampFormat;
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<Event> out) throws IOException {
        if (record.value() == null || record.value().length == 0) {
            log.warn("Received null/empty record from topic={} partition={} offset={}",
                    record.topic(), record.partition(), record.offset());
            return;
        }

        try {
            ensureObjectMapper();

            // Deserialize the top-level JSON into Event (generic LinkedHashMap-based)
            Event event = objectMapper.readValue(record.value(), Event.class);

            // ── Inject routing metadata ────────────────────────────────────────────
            event.put("_source_topic", sourceTopic);
            event.put("_cluster", cluster);
            event.put("_kafka_partition", record.partition());
            event.put("_kafka_offset", record.offset());

            // ── Extract and inject epoch ms timestamp ──────────────────────────────
            long epochMs = extractEpochMs(event, record.timestamp());
            event.put("_event_timestamp_epoch_ms", epochMs);

            // ── Keep _data as raw String for lazy parsing ──────────────────────────
            // The '_data' field is already stored in event.fields as a Map<String,Object>
            // from Jackson deserialization. We serialize it back to a JSON String so that
            // FieldExtractor can lazily parse only the fields actually needed by rules.
            // This avoids holding 130+ parsed fields in heap for filtered-out events.
            Object dataObj = event.getFields().get("_data");
            if (dataObj != null && !(dataObj instanceof String)) {
                // Convert the Map back to compact JSON string
                event.put("_data", objectMapper.writeValueAsString(dataObj));
            }

            out.collect(event);

        } catch (Exception e) {
            log.error("Failed to deserialize event from topic={} offset={}: {}",
                    sourceTopic, record.offset(), e.getMessage());
            // Skip malformed events — do not propagate exception
        }
    }

    /**
     * Extract the event timestamp as epoch milliseconds.
     * Falls back to Kafka record timestamp if extraction fails.
     */
    private long extractEpochMs(Event event, long kafkaTimestamp) {
        Object rawTs = event.getFields().get(eventTimestampField);
        if (rawTs == null) {
            log.debug("Timestamp field '{}' not found in event, using Kafka timestamp", eventTimestampField);
            return kafkaTimestamp;
        }

        try {
            if ("EPOCH_MILLIS".equalsIgnoreCase(eventTimestampFormat)) {
                if (rawTs instanceof Number) {
                    return ((Number) rawTs).longValue();
                }
                return Long.parseLong(rawTs.toString().trim());
            } else {
                // ISO_STRING — delegate to TimeUtils
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