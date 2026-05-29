package in.gov.uidai.dp.velocity.engine.deserializers;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import in.gov.uidai.dp.velocity.engine.model.VelocityRule;
import lombok.extern.slf4j.Slf4j;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.util.Collector;

@Slf4j
public class RuleDeserializer extends RichFlatMapFunction<String, VelocityRule>
        implements ResultTypeQueryable<VelocityRule> {

    private static final long serialVersionUID = 1L;

    private transient ObjectMapper objectMapper;

    @Override
    public void open(OpenContext parameters) throws Exception {
        super.open(parameters);
        objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        log.info("RuleDeserializer initialised");
    }

    @Override
    public void flatMap(String value, Collector<VelocityRule> out) throws Exception {
        if (value == null || value.isBlank()) {
            log.warn("Received null/blank rule message — skipping");
            return;
        }

        try {
            VelocityRule rule = objectMapper.readValue(value.trim(), VelocityRule.class);

            // Validate minimum required fields
            if (rule.getRuleMetadata() == null || rule.getRuleMetadata().getRuleId() == null) {
                log.warn("Rule missing rule_metadata.rule_id — dropping: {}", value);
                return;
            }
            if (rule.getExecutionRouting() == null
                    || rule.getExecutionRouting().getTargetSourceTopic() == null) {
                log.warn("Rule '{}' missing execution_routing.target_source_topic — dropping",
                        rule.getRuleId());
                return;
            }

            // DELETED rules are allowed through — they trigger removal from broadcast state
            if (!rule.isDeleted()) {
                if (rule.getGrouping() == null || rule.getGrouping().getKeys() == null
                        || rule.getGrouping().getKeys().isEmpty()) {
                    log.warn("Rule '{}' has no grouping keys — dropping", rule.getRuleId());
                    return;
                }
                if (rule.getAggregations() == null || rule.getAggregations().isEmpty()) {
                    log.warn("Rule '{}' has no aggregations — dropping", rule.getRuleId());
                    return;
                }
                if (rule.getWindowing() == null) {
                    log.warn("Rule '{}' has no windowing config — dropping", rule.getRuleId());
                    return;
                }
            }

            log.info("Parsed rule: id={} name={} status={} topic={}",
                    rule.getRuleId(), rule.getRuleName(),
                    rule.getStatus(), rule.getTargetSourceTopic());

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