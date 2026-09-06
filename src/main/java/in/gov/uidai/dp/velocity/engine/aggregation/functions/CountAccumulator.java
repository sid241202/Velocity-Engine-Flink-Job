package in.gov.uidai.dp.velocity.engine.aggregation.functions;

import in.gov.uidai.dp.velocity.engine.utils.TimeUtils;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeinfo.Types;

import java.util.Iterator;
import java.util.Map;

public class CountAccumulator {

    private final MapState<String, Long> state;

    public CountAccumulator(RuntimeContext ctx, StateTtlConfig ttlConfig) {
        this(ctx, "count_acc", ttlConfig);
    }

    public CountAccumulator(RuntimeContext ctx, String stateName, StateTtlConfig ttlConfig) {
        MapStateDescriptor<String, Long> desc = new MapStateDescriptor<>(stateName, Types.STRING, Types.LONG);
        desc.enableTimeToLive(ttlConfig);
        this.state = ctx.getMapState(desc);
    }

    public void add(String bucketKey) throws Exception {
        Long current = state.get(bucketKey);
        state.put(bucketKey, (current == null ? 0L : current) + 1L);
    }

    public double computeAndPrune(String alias, long windowStartTs, long windowEndTs, long allowedLatenessMs) throws Exception {
        long total = 0L;
        Iterator<Map.Entry<String, Long>> iter = state.iterator();

        while (iter.hasNext()) {
            Map.Entry<String, Long> entry = iter.next();
            if (alias.equals(TimeUtils.extractAlias(entry.getKey()))) {
                long bucketTs = TimeUtils.extractBucketTs(entry.getKey());
                if (bucketTs < windowStartTs - allowedLatenessMs) {
                    iter.remove();
                } else if (bucketTs >= windowStartTs && bucketTs < windowEndTs) {
                    // Lower bound matters here even though the prune threshold is
                    // windowStartTs - allowedLatenessMs: a bucket kept alive by the
                    // lateness grace period (windowStartTs - allowedLatenessMs <=
                    // bucketTs < windowStartTs) belongs to the PREVIOUS window, not
                    // this one, and must not be counted into this window's total.
                    total += entry.getValue();
                }
            }
        }
        return total;
    }

    public double computeNoPrune(String alias, long windowStartTs, long windowEndTs) throws Exception {
        long total = 0L;
        Iterator<Map.Entry<String, Long>> iter = state.iterator();

        while (iter.hasNext()) {
            Map.Entry<String, Long> entry = iter.next();
            if (alias.equals(TimeUtils.extractAlias(entry.getKey()))) {
                long bucketTs = TimeUtils.extractBucketTs(entry.getKey());
                if (bucketTs >= windowStartTs && bucketTs < windowEndTs) {
                    total += entry.getValue();
                }
            }
        }
        return total;
    }

    public boolean isEmpty() throws Exception {
        return state.isEmpty();
    }

    /** Number of distinct bucket keys currently held — used for the
     * velocity_window_state_keys state-bloat gauge. */
    public long keyCount() throws Exception {
        long count = 0L;
        for (String ignored : state.keys()) count++;
        return count;
    }
}