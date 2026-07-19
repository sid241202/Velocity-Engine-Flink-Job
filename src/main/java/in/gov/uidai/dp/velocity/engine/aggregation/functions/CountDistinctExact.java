package in.gov.uidai.dp.velocity.engine.aggregation.functions;

import in.gov.uidai.dp.velocity.engine.utils.TimeUtils;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeinfo.Types;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Exact distinct-count accumulator.
 *
 * Storage layout (v3): instead of one MapState entry per bucket holding every
 * distinct value concatenated into a single delimiter-joined String (which made
 * {@code add()} O(n) — read blob, split into a Set, re-join, rewrite the whole
 * up-to-10MB blob to RocksDB on every event), membership is now one MapState
 * entry per (bucket, value):
 *
 *   memberState : key = "{bucketKey}<NUL>{value}"  ->  TRUE
 *   countState  : key = "{bucketKey}"               ->  distinct count in that bucket
 *
 * {@code add()} is now O(1): a single point lookup + two point writes. The
 * companion countState lets us enforce the per-bucket cap without scanning.
 *
 * NOTE: state descriptor names were bumped v2 -> v3 because the value type of
 * the membership map changed (String -> Boolean). On restore from a pre-v3
 * savepoint the old "distinct_exact_acc_v2" state is simply not read (it TTLs
 * away), and v3 starts fresh — the safe migration path for an incompatible
 * state-schema change. The only visible effect is that distinct counts for
 * windows still open at the moment of the upgrade start from zero.
 */
public class CountDistinctExact {

    // Null byte: cannot appear in a validated alias or an Aadhaar-like value, so
    // the FIRST occurrence always cleanly separates bucketKey from value even
    // when the value itself contains spaces or '#'.
    private static final String DELIMITER = String.valueOf((char) 0);

    /**
     * Hard cap on distinct values stored per bucket. At 80M events/day a single
     * high-cardinality bucket could otherwise grow unboundedly. With a 50K cap and
     * ~200 bytes per Aadhaar-like value, max state per bucket ≈ 10MB. Values beyond
     * the cap are ignored (the count saturates at MAX_DISTINCT_PER_BUCKET).
     */
    private static final int MAX_DISTINCT_PER_BUCKET = 50_000;

    private final MapState<String, Boolean> memberState;
    private final MapState<String, Long> countState;

    public CountDistinctExact(RuntimeContext ctx, StateTtlConfig ttlConfig) {
        MapStateDescriptor<String, Boolean> memberDesc =
                new MapStateDescriptor<>("distinct_exact_acc_v3", Types.STRING, Types.BOOLEAN);
        memberDesc.enableTimeToLive(ttlConfig);
        this.memberState = ctx.getMapState(memberDesc);

        MapStateDescriptor<String, Long> countDesc =
                new MapStateDescriptor<>("distinct_exact_count_v3", Types.STRING, Types.LONG);
        countDesc.enableTimeToLive(ttlConfig);
        this.countState = ctx.getMapState(countDesc);
    }

    public void add(String bucketKey, String value) throws Exception {
        if (value == null || value.isEmpty()) return;

        String memberKey = bucketKey + DELIMITER + value;
        // Already counted this value for this bucket — idempotent, keeps count exact.
        if (Boolean.TRUE.equals(memberState.get(memberKey))) return;

        Long current = countState.get(bucketKey);
        long count = (current == null) ? 0L : current;
        // Enforce cap BEFORE adding to prevent unbounded state growth.
        if (count >= MAX_DISTINCT_PER_BUCKET) return; // silently saturate

        memberState.put(memberKey, Boolean.TRUE);
        countState.put(bucketKey, count + 1L);
    }

    public double computeAndPrune(String alias, long windowStartTs, long windowEndTs, long allowedLatenessMs) throws Exception {
        Set<String> globalSet = new HashSet<>();
        Set<String> expiredBuckets = new HashSet<>();
        long pruneBefore = windowStartTs - allowedLatenessMs;

        Iterator<Map.Entry<String, Boolean>> iter = memberState.iterator();
        while (iter.hasNext()) {
            Map.Entry<String, Boolean> entry = iter.next();
            int sep = entry.getKey().indexOf(DELIMITER);
            if (sep < 0) continue; // malformed — skip defensively
            String bucketKey = entry.getKey().substring(0, sep);
            if (!alias.equals(TimeUtils.extractAlias(bucketKey))) continue;

            long bucketTs = TimeUtils.extractBucketTs(bucketKey);
            if (bucketTs < pruneBefore) {
                iter.remove();
                expiredBuckets.add(bucketKey);
            } else if (bucketTs >= windowStartTs && bucketTs < windowEndTs) {
                // Lower bound matters here even though the prune threshold is
                // pruneBefore (windowStartTs - allowedLatenessMs): a bucket kept
                // alive by the lateness grace period belongs to the PREVIOUS
                // window, not this one.
                globalSet.add(entry.getKey().substring(sep + DELIMITER.length()));
            }
        }

        // Drop the companion per-bucket counts for buckets we just fully pruned.
        for (String bk : expiredBuckets) {
            countState.remove(bk);
        }
        return globalSet.size();
    }

    public double computeNoPrune(String alias, long windowStartTs, long windowEndTs) throws Exception {
        Set<String> globalSet = new HashSet<>();

        Iterator<Map.Entry<String, Boolean>> iter = memberState.iterator();
        while (iter.hasNext()) {
            Map.Entry<String, Boolean> entry = iter.next();
            int sep = entry.getKey().indexOf(DELIMITER);
            if (sep < 0) continue;
            String bucketKey = entry.getKey().substring(0, sep);
            if (!alias.equals(TimeUtils.extractAlias(bucketKey))) continue;

            long bucketTs = TimeUtils.extractBucketTs(bucketKey);
            if (bucketTs >= windowStartTs && bucketTs < windowEndTs) {
                globalSet.add(entry.getKey().substring(sep + DELIMITER.length()));
            }
        }
        return globalSet.size();
    }

    public boolean isEmpty() throws Exception {
        return memberState.isEmpty();
    }
}
