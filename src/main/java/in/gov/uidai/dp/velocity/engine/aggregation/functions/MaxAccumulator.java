package in.gov.uidai.dp.velocity.engine.aggregation.functions;

import in.gov.uidai.dp.velocity.engine.utils.TimeUtils;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;

import java.util.Iterator;
import java.util.Map;

public class MaxAccumulator {

    private final MapState<String, Double> state;

    public MaxAccumulator(RuntimeContext ctx) {
        MapStateDescriptor<String, Double> desc = new MapStateDescriptor<>("max_acc", Types.STRING, Types.DOUBLE);
        this.state = ctx.getMapState(desc);
    }

    public void add(String bucketKey, double value) throws Exception {
        Double current = state.get(bucketKey);
        if (current == null || value > current) {
            state.put(bucketKey, value);
        }
    }

    public double computeAndPrune(String alias, long windowStartTs, long windowEndTs) throws Exception {
        double max = -Double.MAX_VALUE;
        boolean found = false;
        Iterator<Map.Entry<String, Double>> iter = state.iterator();

        while (iter.hasNext()) {
            Map.Entry<String, Double> entry = iter.next();
            if (alias.equals(TimeUtils.extractAlias(entry.getKey()))) {
                long bucketTs = TimeUtils.extractBucketTs(entry.getKey());
                if (bucketTs < windowStartTs) {
                    iter.remove();
                } else if (bucketTs < windowEndTs) {
                    found = true;
                    if (entry.getValue() > max) max = entry.getValue();
                }
            }
        }
        return found ? max : 0.0;
    }
}