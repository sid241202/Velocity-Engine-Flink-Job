package in.gov.uidai.dp.velocity.engine.sinks;

import in.gov.uidai.dp.velocity.engine.model.AggregationResult;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ClickHouseResultConverter {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private ClickHouseResultConverter() {}

    public static String toJsonEachRow(AggregationResult result) {
        try {
            return MAPPER.writeValueAsString(result);
        } catch (JsonProcessingException e) {
            org.slf4j.LoggerFactory.getLogger(ClickHouseResultConverter.class)
                .error("Failed to serialize AggregationResult to JSON: {}", e.getMessage());
            return null;
        }
    }
}