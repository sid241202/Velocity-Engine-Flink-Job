package in.gov.uidai.dp.velocity.engine.config;

import lombok.Builder;
import lombok.Value;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

// Describes one ClickHouse table Flink owns end to end: DDL (local
// ReplicatedReplacingMergeTree + Distributed, both ON CLUSTER) and the write
// target (the Distributed table — see forAggResults/forAnomalyEvents for the
// two concrete tables, ported from the notebook's DDL/MV cells).
@Value
@Builder
public class ClickHouseSinkConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    List<String> hosts;
    String user;
    String password;

    // Local (per-shard) ReplicatedReplacingMergeTree table.
    String localDatabase;
    String localTable;

    // Distributed table — this is what Flink INSERTs into.
    String database;
    String table;

    String clusterName;
    // ReplicatedReplacingMergeTree version column (e.g. "producedAt"). Null/empty
    // means no explicit version arg (ClickHouse falls back to insertion order).
    String versionColumn;
    String columnsDdl;
    String orderByColumns;
    String partitionByExpr;
    String ttlExpr;
    String shardingKeyExpr;

    @Builder.Default
    int maxBufferSize = 5000;
    @Builder.Default
    long flushIntervalMs = 5000L;
    @Builder.Default
    boolean autoCreateDdl = true;

    public String getFirstHost() {
        return (hosts != null && !hosts.isEmpty()) ? hosts.get(0) : null;
    }

    public String getFirstHostUrl() {
        String h = getFirstHost();
        return h != null ? "http://" + h : null;
    }

    // Ported from the notebook's cells 63/64 (auth_velocity_agg_results_rmt +
    // Distributed auth_velocity_agg_results). Flink now owns this table end to
    // end in place of the ClickHouse-native Kafka Engine + MV pair.
    public static ClickHouseSinkConfig forAggResults() {
        return ClickHouseSinkConfig.builder()
                .hosts(parseHosts(AuthDemoConfig.CH_HOSTS))
                .user(AuthDemoConfig.CH_USER)
                .password(AuthDemoConfig.CH_PASSWORD)
                .localDatabase("auth_stream_rmt")
                .localTable(AuthDemoConfig.CH_AGG_TABLE + "_rmt")
                .database(AuthDemoConfig.CH_DATABASE)
                .table(AuthDemoConfig.CH_AGG_TABLE)
                .clusterName("rmt_cluster")
                .versionColumn("producedAt")
                .columnsDdl("""
                        ruleId              LowCardinality(String),
                        windowStart         DateTime64(3, 'Asia/Kolkata'),
                        windowEnd           DateTime64(3, 'Asia/Kolkata'),
                        entityName          LowCardinality(String),
                        groupKey            String,
                        aggResult           String,
                        producedAt          DateTime64(3, 'Asia/Kolkata'),
                        thresholdBreached   Bool,
                        isFinal             Bool DEFAULT true""")
                .orderByColumns("(ruleId, windowStart, groupKey)")
                .partitionByExpr("toYYYYMMDD(windowStart)")
                // Cut from 90 to 1 day — the Aggregated Analytics UI now caps its
                // own date-range picker at the last 24 hours to match (see
                // AggregatedAnalysis.jsx's minDate), so there's no reachable path
                // that would ever query a row older than this TTL anyway.
                .ttlExpr("toDate(windowStart) + INTERVAL 1 DAY DELETE")
                .shardingKeyExpr("cityHash64(ruleId)")
                .maxBufferSize(AuthDemoConfig.CH_BATCH_SIZE)
                .flushIntervalMs(AuthDemoConfig.CH_FLUSH_INTERVAL_MS)
                .autoCreateDdl(true)
                .build();
    }

    // Ported from the notebook's cells 65/66 (auth_velocity_anomaly_events_rmt +
    // Distributed auth_velocity_anomaly_events). One deliberate deviation from
    // the notebook: it declares this table's ReplicatedReplacingMergeTree with
    // NO version column (asymmetric with the agg-results table, which versions
    // on producedAt). Without one, ClickHouse's merge-time dedup falls back to
    // insertion order rather than "latest producedAt wins" — a weaker, less
    // predictable guarantee. Fixed here by giving it the same producedAt
    // version column as the agg-results table.
    public static ClickHouseSinkConfig forAnomalyEvents() {
        return ClickHouseSinkConfig.builder()
                .hosts(parseHosts(AuthDemoConfig.CH_HOSTS))
                .user(AuthDemoConfig.CH_USER)
                .password(AuthDemoConfig.CH_PASSWORD)
                .localDatabase("auth_stream_rmt")
                .localTable(AuthDemoConfig.CH_ANOMALY_TABLE + "_rmt")
                .database(AuthDemoConfig.CH_DATABASE)
                .table(AuthDemoConfig.CH_ANOMALY_TABLE)
                .clusterName("rmt_cluster")
                .versionColumn("producedAt")
                .columnsDdl("""
                        ruleId              LowCardinality(String),
                        entityValue         String,
                        producedAt          DateTime64(3, 'Asia/Kolkata'),
                        penaltyTtlSeconds   UInt32""")
                .orderByColumns("(ruleId, entityValue, producedAt)")
                .partitionByExpr("toYYYYMMDD(producedAt)")
                .ttlExpr("toDate(producedAt) + INTERVAL 180 DAY DELETE")
                .shardingKeyExpr("cityHash64(ruleId)")
                .maxBufferSize(AuthDemoConfig.CH_BATCH_SIZE)
                .flushIntervalMs(AuthDemoConfig.CH_FLUSH_INTERVAL_MS)
                .autoCreateDdl(true)
                .build();
    }

    private static List<String> parseHosts(String hosts) {
        return Arrays.stream(hosts.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toList());
    }
}
