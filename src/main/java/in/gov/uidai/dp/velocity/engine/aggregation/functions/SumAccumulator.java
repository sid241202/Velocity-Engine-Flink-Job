package in.gov.uidai.dp.velocity.engine.aggregation.functions;

import in.gov.uidai.dp.velocity.engine.utils.TimeUtils;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;

import java.util.Iterator;
import java.util.Map;

public class SumAccumulator {

    private final MapState<String, Double> state;

    public SumAccumulator(RuntimeContext ctx) {
        MapStateDescriptor<String, Double> desc = new MapStateDescriptor<>("sum_acc", Types.STRING, Types.DOUBLE);
        this.state = ctx.getMapState(desc);
    }

    public void add(String bucketKey, double value) throws Exception {
        Double current = state.get(bucketKey);
        state.put(bucketKey, (current == null ? 0.0 : current) + value);
    }

    public double computeAndPrune(String alias, long windowStartTs, long windowEndTs) throws Exception {
        double total = 0.0;
        Iterator<Map.Entry<String, Double>> iter = state.iterator();

        while (iter.hasNext()) {
            Map.Entry<String, Double> entry = iter.next();
            if (alias.equals(TimeUtils.extractAlias(entry.getKey()))) {
                long bucketTs = TimeUtils.extractBucketTs(entry.getKey());
                if (bucketTs < windowStartTs) {
                    iter.remove();
                } else if (bucketTs < windowEndTs) {
                    total += entry.getValue();
                }
            }
        }
        return total;
    }
}