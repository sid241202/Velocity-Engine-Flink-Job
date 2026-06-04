package in.gov.uidai.dp.velocity.engine.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class HavingThresholds implements Serializable {
    private static final long serialVersionUID = 1L;
    private String expression; // JEXL expression, e.g. "(total_count >= 50) && (unique_uids > 10)"
}