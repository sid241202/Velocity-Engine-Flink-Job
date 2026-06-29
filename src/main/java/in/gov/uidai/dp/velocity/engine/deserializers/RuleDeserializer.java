package in.gov.uidai.dp.velocity.engine.deserializers;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import in.gov.uidai.dp.velocity.engine.model.VelocityRule;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

@Slf4j
public class RuleDeserializer implements KafkaRecordDeserializationSchema<VelocityRule> {

    private static final long serialVersionUID = 1L;

    private transient ObjectMapper objectMapper;

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<VelocityRule> out) throws IOException {
        if (record.value() == null || record.value().length == 0) {
            log.warn("Received null/blank rule message — skipping");
            return;
        }

        String value = new String(record.value(), StandardCharsets.UTF_8);

        try {
            if (objectMapper == null) {
                objectMapper = new ObjectMapper();
                objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            }
            VelocityRule rule = objectMapper.readValue(value.trim(), VelocityRule.class);

            if (rule.getRuleMetadata() == null || rule.getRuleMetadata().getRuleId() == null) {
                log.warn("Rule missing rule_metadata.rule_id — dropping: {}", value);
                return;
            }
            if (rule.getExecutionRouting() == null
                    || rule.getSourceTopic() == null) {
                log.warn("Rule '{}' missing execution routing or source topic — dropping",
                        rule.getRuleId());
                return;
            }

            if (!rule.isDeleted()) {
                if (rule.getGrouping() == null || rule.getGrouping().getKeys() == null
                        || rule.getGrouping().getKeys().isEmpty()) {
                    log.warn("Rule '{}' has no grouping keys — dropping", rule.getRuleId());
                    return;
                }
                boolean isNoWindowing = rule.getWindowing() != null && rule.getWindowing().isNoWindowing();
                if (!isNoWindowing && (rule.getAggregations() == null || rule.getAggregations().isEmpty())) {
                    log.warn("Rule '{}' has no aggregations (windowed mode requires them) — dropping", rule.getRuleId());
                    return;
                }
                if (rule.getWindowing() == null) {
                    log.warn("Rule '{}' has no windowing config — dropping", rule.getRuleId());
                    return;
                }
            }

            log.info("Successfully deserialized Rule {} (Status: {}, Target: {})",
                    rule.getRuleId(),
                    rule.getStatus(), rule.getSourceTopic());

            out.collect(rule);

        } catch (Exception e) {
            log.warn("Failed to parse rule JSON — dropping. Error: {}. Payload snippet: {}",
                    e.getMessage(),
                    value.length() > 200 ? value.substring(0, 200) + "..." : value);
        }
    }

    @Override
    public TypeInformation<VelocityRule> getProducedType() {
        return TypeInformation.of(VelocityRule.class);
    }
}