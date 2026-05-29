package in.gov.uidai.dp.velocity.engine.aggregation.functions;

import in.gov.uidai.dp.velocity.engine.utils.TimeUtils;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;

import java.util.Iterator;
import java.util.Map;

public class CountAccumulator {

    private final MapState<String, Long> state;

    public CountAccumulator(RuntimeContext ctx) {
        MapStateDescriptor<String, Long> desc = new MapStateDescriptor<>("count_acc", Types.STRING, Types.LONG);
        this.state = ctx.getMapState(desc);
    }

    public void add(String bucketKey) throws Exception {
        Long current = state.get(bucketKey);
        state.put(bucketKey, (current == null ? 0L : current) + 1L);
    }

    public double computeAndPrune(String alias, long windowStartTs, long windowEndTs) throws Exception {
        long total = 0L;
        Iterator<Map.Entry<String, Long>> iter = state.iterator();

        while (iter.hasNext()) {
            Map.Entry<String, Long> entry = iter.next();
            if (alias.equals(TimeUtils.extractAlias(entry.getKey()))) {
                long bucketTs = TimeUtils.extractBucketTs(entry.getKey());
                if (bucketTs < windowStartTs) {
                    iter.remove(); // prune expired
                } else if (bucketTs < windowEndTs) {
                    total += entry.getValue();
                }
            }
        }
        return total;
    }
}