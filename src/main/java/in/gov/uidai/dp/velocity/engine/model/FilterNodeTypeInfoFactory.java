package in.gov.uidai.dp.velocity.engine.model;

import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;

import java.lang.reflect.Type;
import java.util.Map;

public class FilterNodeTypeInfoFactory extends TypeInfoFactory<FilterNode> {
    @Override
    public TypeInformation<FilterNode> createTypeInfo(Type t, Map<String, TypeInformation<?>> genericParameters) {
        return new GenericTypeInfo<>(FilterNode.class);
    }
}
