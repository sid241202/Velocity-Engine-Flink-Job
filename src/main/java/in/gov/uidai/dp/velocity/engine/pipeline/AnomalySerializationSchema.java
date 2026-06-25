package in.gov.uidai.dp.velocity.engine.pipeline;

import in.gov.uidai.dp.velocity.engine.model.AnomalyEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SerializationSchema;

@Slf4j
public class AnomalySerializationSchema implements SerializationSchema<AnomalyEvent> {
    private static final long serialVersionUID = 1L;
    private transient ObjectMapper mapper;

    @Override
    public void open(InitializationContext context) { mapper = new ObjectMapper(); }

    @Override
    public byte[] serialize(AnomalyEvent event) {
        try {
            if (mapper == null) mapper = new ObjectMapper();
            return mapper.writeValueAsBytes(event);
        } catch (Exception e) {
            log.error("Failed to serialize AnomalyEvent id={}: {}", event.getId(), e.getMessage());
            return new byte[0];
        }
    }
}
