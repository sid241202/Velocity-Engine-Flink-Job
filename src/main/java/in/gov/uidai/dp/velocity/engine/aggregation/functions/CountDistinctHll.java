package in.gov.uidai.dp.velocity.engine.aggregation.functions;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import in.gov.uidai.dp.velocity.engine.utils.TimeUtils;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.Types;

import java.util.Iterator;
import java.util.Map;

public class CountDistinctHll {

    private final MapState<String, byte[]> state;

    public CountDistinctHll(RuntimeContext ctx) {
        MapStateDescriptor<String, byte[]> desc = new MapStateDescriptor<>(
                "distinct_hll_acc", Types.STRING, PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO
        );
        this.state = ctx.getMapState(desc);
    }

    public void add(String bucketKey, String value) throws Exception {
        byte[] current = state.get(bucketKey);
        HyperLogLog hll;
        if (current == null) {
            hll = new HyperLogLog(14);
        } else {
            hll = HyperLogLog.Builder.build(current);
        }
        hll.offer(value);
        state.put(bucketKey, hll.getBytes());
    }

    public double computeAndPrune(String alias, long windowStartTs, long windowEndTs, long allowedLatenessMs) throws Exception {
        HyperLogLog globalHll = new HyperLogLog(14);
        Iterator<Map.Entry<String, byte[]>> iter = state.iterator();

        while (iter.hasNext()) {
            Map.Entry<String, byte[]> entry = iter.next();
            if (alias.equals(TimeUtils.extractAlias(entry.getKey()))) {
                long bucketTs = TimeUtils.extractBucketTs(entry.getKey());
                if (bucketTs < windowStartTs - allowedLatenessMs) {
                    iter.remove();
                } else if (bucketTs < windowEndTs) {
                    HyperLogLog hll = HyperLogLog.Builder.build(entry.getValue());
                    globalHll.addAll(hll);
                }
            }
        }
        return globalHll.cardinality();
    }

    public double computeNoPrune(String alias, long windowStartTs, long windowEndTs) throws Exception {
        HyperLogLog globalHll = new HyperLogLog(14);
        Iterator<Map.Entry<String, byte[]>> iter = state.iterator();

        while (iter.hasNext()) {
            Map.Entry<String, byte[]> entry = iter.next();
            if (alias.equals(TimeUtils.extractAlias(entry.getKey()))) {
                long bucketTs = TimeUtils.extractBucketTs(entry.getKey());
                if (bucketTs >= windowStartTs && bucketTs < windowEndTs) {
                    HyperLogLog hll = HyperLogLog.Builder.build(entry.getValue());
                    globalHll.addAll(hll);
                }
            }
        }
        return globalHll.cardinality();
    }

    public boolean isEmpty() throws Exception {
        return state.isEmpty();
    }
}