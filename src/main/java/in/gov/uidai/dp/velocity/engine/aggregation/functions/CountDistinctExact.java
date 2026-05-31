package in.gov.uidai.dp.velocity.engine.aggregation.functions;

import in.gov.uidai.dp.velocity.engine.utils.TimeUtils;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class CountDistinctExact {

    private final MapState<String, String> state;

    public CountDistinctExact(RuntimeContext ctx) {

        MapStateDescriptor<String, String> desc = new MapStateDescriptor<>("distinct_exact_acc", Types.STRING, Types.STRING);
        this.state = ctx.getMapState(desc);
    }

    public void add(String bucketKey, String value) throws Exception {
        String current = state.get(bucketKey);
        if (current == null) {
            state.put(bucketKey, value);
        } else {

            if (!current.equals(value) && !current.contains(value + ",") && !current.contains("," + value) && !current.endsWith("," + value)) {

                Set<String> set = new HashSet<>(java.util.Arrays.asList(current.split(",")));
                if (set.add(value)) {
                    state.put(bucketKey, current + "," + value);
                }
            }
        }
    }

    public double computeAndPrune(String alias, long windowStartTs, long windowEndTs) throws Exception {
        Set<String> globalSet = new HashSet<>();
        Iterator<Map.Entry<String, String>> iter = state.iterator();

        while (iter.hasNext()) {
            Map.Entry<String, String> entry = iter.next();
            if (alias.equals(TimeUtils.extractAlias(entry.getKey()))) {
                long bucketTs = TimeUtils.extractBucketTs(entry.getKey());
                if (bucketTs < windowStartTs) {
                    iter.remove();
                } else if (bucketTs < windowEndTs) {
                    String[] values = entry.getValue().split(",");
                    for (String val : values) {
                        if (!val.isEmpty()) globalSet.add(val);
                    }
                }
            }
        }
        return globalSet.size();
    }
    public boolean isEmpty() throws Exception {
        return state.isEmpty();
    }
}