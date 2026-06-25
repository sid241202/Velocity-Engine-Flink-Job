package in.gov.uidai.dp.velocity.engine.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AnomalyEvent implements Serializable {
    private static final long serialVersionUID = 1L;
    private String id;
    private String entityValue;
    private String producedAt;
    private int penaltyTtlSeconds; // Redis key TTL from rule_metadata — 0 = use sink default
}
