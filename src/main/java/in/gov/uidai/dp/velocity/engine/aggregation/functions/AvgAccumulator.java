package in.gov.uidai.dp.velocity.engine.aggregation.functions;

import in.gov.uidai.dp.velocity.engine.utils.TimeUtils;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Iterator;
import java.util.Map;

public class AvgAccumulator {

    private final MapState<String, Tuple2<Double, Long>> state;

    public AvgAccumulator(RuntimeContext ctx, StateTtlConfig ttlConfig) {
        MapStateDescriptor<String, Tuple2<Double, Long>> desc = new MapStateDescriptor<>(
                "avg_acc", Types.STRING, TypeInformation.of(new TypeHint<Tuple2<Double, Long>>() {})
        );
        desc.enableTimeToLive(ttlConfig);
        this.state = ctx.getMapState(desc);
    }

    public void add(String bucketKey, double value) throws Exception {
        Tuple2<Double, Long> current = state.get(bucketKey);
        if (current == null) {
            state.put(bucketKey, Tuple2.of(value, 1L));
        } else {
            current.f0 += value;
            current.f1 += 1L;
            state.put(bucketKey, current);
        }
    }

    public double computeAndPrune(String alias, long windowStartTs, long windowEndTs, long allowedLatenessMs) throws Exception {
        double sum = 0.0;
        long count = 0L;
        Iterator<Map.Entry<String, Tuple2<Double, Long>>> iter = state.iterator();

        while (iter.hasNext()) {
            Map.Entry<String, Tuple2<Double, Long>> entry = iter.next();
            if (alias.equals(TimeUtils.extractAlias(entry.getKey()))) {
                long bucketTs = TimeUtils.extractBucketTs(entry.getKey());
                if (bucketTs < windowStartTs - allowedLatenessMs) {
                    iter.remove();
                } else if (bucketTs >= windowStartTs && bucketTs < windowEndTs) {
                    // Lower bound matters here even though the prune threshold is
                    // windowStartTs - allowedLatenessMs: a bucket kept alive by the
                    // lateness grace period belongs to the PREVIOUS window, not this one.
                    sum += entry.getValue().f0;
                    count += entry.getValue().f1;
                }
            }
        }
        return count == 0 ? 0.0 : sum / count;
    }

    public double computeNoPrune(String alias, long windowStartTs, long windowEndTs) throws Exception {
        double sum = 0.0;
        long count = 0L;
        Iterator<Map.Entry<String, Tuple2<Double, Long>>> iter = state.iterator();

        while (iter.hasNext()) {
            Map.Entry<String, Tuple2<Double, Long>> entry = iter.next();
            if (alias.equals(TimeUtils.extractAlias(entry.getKey()))) {
                long bucketTs = TimeUtils.extractBucketTs(entry.getKey());
                if (bucketTs >= windowStartTs && bucketTs < windowEndTs) {
                    sum += entry.getValue().f0;
                    count += entry.getValue().f1;
                }
            }
        }
        return count == 0 ? 0.0 : sum / count;
    }

    public boolean isEmpty() throws Exception {
        return state.isEmpty();
    }
}