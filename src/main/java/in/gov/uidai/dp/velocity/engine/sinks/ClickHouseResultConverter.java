package in.gov.uidai.dp.velocity.engine.sinks;

import in.gov.uidai.dp.velocity.engine.model.AggregationResult;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class ClickHouseResultConverter {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private ClickHouseResultConverter() {}

    public static String toJsonEachRow(AggregationResult result) {
        try {
            // Create a tree copy so we can override aggregationResults as a JSON string
            ObjectNode node = MAPPER.valueToTree(result);
            // aggregationResults is a Map — serialize it to a JSON string value for the String column in ClickHouse
            if (result.getAggregationResults() != null) {
                node.put("aggregationResults", MAPPER.writeValueAsString(result.getAggregationResults()));
            }
            return MAPPER.writeValueAsString(node);
        } catch (JsonProcessingException e) {
            org.slf4j.LoggerFactory.getLogger(ClickHouseResultConverter.class)
                .error("Failed to serialize AggregationResult to JSON: {}", e.getMessage());
            return null;
        }
    }
}