package in.gov.uidai.dp.velocity.engine.pipeline;

import in.gov.uidai.dp.velocity.engine.model.AggregationResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SerializationSchema;

@Slf4j
public class ResultSerializationSchema implements SerializationSchema<AggregationResult> {
    private static final long serialVersionUID = 2L;
    private transient ObjectMapper mapper;

    @Override
    public void open(InitializationContext context) { mapper = new ObjectMapper(); }

    @Override
    public byte[] serialize(AggregationResult result) {
        try {
            if (mapper == null) mapper = new ObjectMapper();
            return mapper.writeValueAsBytes(result);
        } catch (Exception e) {
            log.error("Failed to serialize AggregationResult id={}: {}", result.getId(), e.getMessage());
            return null; // Kafka sinks skip null records
        }
    }
}
