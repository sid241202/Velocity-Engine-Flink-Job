package in.gov.uidai.dp.velocity.engine.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

@Data @NoArgsConstructor @AllArgsConstructor @JsonIgnoreProperties(ignoreUnknown = true)
public class GroupingConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    /** Field paths used to build the composite grouping key. E.g. ["_data.enrolmentReferenceId","_data.deviceCode"] */
    private List<String> keys;
}