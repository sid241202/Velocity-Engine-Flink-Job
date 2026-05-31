package in.gov.uidai.dp.velocity.engine.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class Keyed<IN, KEY, ID> implements Serializable {

    private static final long serialVersionUID = 1L;

    private IN  wrapped;
    private KEY key;
    private ID  id;
}