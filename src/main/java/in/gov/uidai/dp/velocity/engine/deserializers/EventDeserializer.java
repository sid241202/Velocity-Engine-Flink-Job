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

@Slf4j
public class EventDeserializer implements KafkaRecordDeserializationSchema<Event> {

    private static final long serialVersionUID = 1L;

    private final String sourceTopic;
    private final String cluster;
    private final String eventTimestampField;
    private final String eventTimestampFormat;

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

            Event event = objectMapper.readValue(record.value(), Event.class);

            event.put("_source_topic", sourceTopic);
            event.put("_cluster", cluster);
            event.put("_kafka_partition", record.partition());
            event.put("_kafka_offset", record.offset());

            long epochMs = extractEpochMs(event, record.timestamp());
            event.put("_event_timestamp_epoch_ms", epochMs);

            Object dataObj = event.getFields().get("_data");
            if (dataObj != null && !(dataObj instanceof String)) {

                event.put("_data", objectMapper.writeValueAsString(dataObj));
            }

            out.collect(event);

        } catch (Exception e) {
            log.error("Failed to deserialize event from topic={} offset={}: {}",
                    sourceTopic, record.offset(), e.getMessage());

        }
    }

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