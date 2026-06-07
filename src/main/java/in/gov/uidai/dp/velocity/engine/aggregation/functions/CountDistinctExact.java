package in.gov.uidai.dp.velocity.engine.aggregation.functions;

import in.gov.uidai.dp.velocity.engine.utils.TimeUtils;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeinfo.Types;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class CountDistinctExact {

    private static final String DELIMITER = "\u0000";
    private final MapState<String, String> state;

    public CountDistinctExact(RuntimeContext ctx, StateTtlConfig ttlConfig) {
        MapStateDescriptor<String, String> desc = new MapStateDescriptor<>("distinct_exact_acc", Types.STRING, Types.STRING);
        desc.enableTimeToLive(ttlConfig);
        this.state = ctx.getMapState(desc);
    }

    public void add(String bucketKey, String value) throws Exception {
        String current = state.get(bucketKey);
        if (current == null) {
            state.put(bucketKey, value);
            return;
        }
        Set<String> set = new HashSet<>(Arrays.asList(current.split(DELIMITER, -1)));
        if (set.add(value)) {
            state.put(bucketKey, String.join(DELIMITER, set));
        }
    }

    public double computeAndPrune(String alias, long windowStartTs, long windowEndTs, long allowedLatenessMs) throws Exception {
        Set<String> globalSet = new HashSet<>();
        Iterator<Map.Entry<String, String>> iter = state.iterator();

        while (iter.hasNext()) {
            Map.Entry<String, String> entry = iter.next();
            if (alias.equals(TimeUtils.extractAlias(entry.getKey()))) {
                long bucketTs = TimeUtils.extractBucketTs(entry.getKey());
                if (bucketTs < windowStartTs - allowedLatenessMs) {
                    iter.remove();
                } else if (bucketTs < windowEndTs) {
                    String[] values = entry.getValue().split(DELIMITER, -1);
                    for (String val : values) {
                        if (!val.isEmpty()) globalSet.add(val);
                    }
                }
            }
        }
        return globalSet.size();
    }

    public double computeNoPrune(String alias, long windowStartTs, long windowEndTs) throws Exception {
        Set<String> globalSet = new HashSet<>();
        Iterator<Map.Entry<String, String>> iter = state.iterator();

        while (iter.hasNext()) {
            Map.Entry<String, String> entry = iter.next();
            if (alias.equals(TimeUtils.extractAlias(entry.getKey()))) {
                long bucketTs = TimeUtils.extractBucketTs(entry.getKey());
                if (bucketTs >= windowStartTs && bucketTs < windowEndTs) {
                    String[] values = entry.getValue().split(DELIMITER, -1);
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