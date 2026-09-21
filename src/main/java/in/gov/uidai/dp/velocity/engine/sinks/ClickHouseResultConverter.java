package in.gov.uidai.dp.velocity.engine.sinks;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import in.gov.uidai.dp.velocity.engine.model.AggregationResult;
import in.gov.uidai.dp.velocity.engine.model.AnomalyEvent;
import lombok.extern.slf4j.Slf4j;

// Maps Flink's Kafka-bound models to the JSON row shape the notebook's
// ClickHouse tables expect. Deliberately NOT a passthrough
// MAPPER.writeValueAsString(model) — both models key the rule by "id" (the
// Kafka RESULTS/ANOMALIES topic contract with the backend consumer, must
// stay "id"), but the ClickHouse tables' column is "ruleId", and
// AggregationResult carries an "entityValue" field the target table doesn't
// have (dropped here, matching the notebook MV's column list exactly).
@Slf4j
public final class ClickHouseResultConverter {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private ClickHouseResultConverter() {}

    public static String toAggRow(AggregationResult r) {
        if (r == null || r.getId() == null) {
            log.warn("Skipping AggregationResult with no id — cannot map to ClickHouse ruleId column");
            return null;
        }
        ObjectNode node = MAPPER.createObjectNode();
        node.put("ruleId", r.getId());
        node.put("windowStart", r.getWindowStart());
        node.put("windowEnd", r.getWindowEnd());
        node.put("entityName", r.getEntityName());
        node.put("groupKey", r.getGroupKey());
        node.put("aggResult", r.getAggResult());
        node.put("producedAt", r.getProducedAt());
        node.put("thresholdBreached", r.isThresholdBreached());
        node.put("isFinal", r.isFinal());
        return writeOrNull(node);
    }

    public static String toAnomalyRow(AnomalyEvent e) {
        if (e == null || e.getId() == null) {
            log.warn("Skipping AnomalyEvent with no id — cannot map to ClickHouse ruleId column");
            return null;
        }
        ObjectNode node = MAPPER.createObjectNode();
        node.put("ruleId", e.getId());
        node.put("entityValue", e.getEntityValue());
        node.put("producedAt", e.getProducedAt());
        node.put("penaltyTtlSeconds", e.getPenaltyTtlSeconds());
        return writeOrNull(node);
    }

    private static String writeOrNull(ObjectNode node) {
        try {
            return MAPPER.writeValueAsString(node);
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize ClickHouse row: {}", e.getMessage());
            return null;
        }
    }
}
