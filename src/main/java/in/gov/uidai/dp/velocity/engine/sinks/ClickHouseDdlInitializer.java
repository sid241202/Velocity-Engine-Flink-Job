package in.gov.uidai.dp.velocity.engine.sinks;

import in.gov.uidai.dp.velocity.engine.config.ClickHouseSinkConfig;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

// Create-if-missing DDL for one ClickHouseSinkConfig-described table: a local
// ReplicatedReplacingMergeTree (one per shard) plus a Distributed table over
// it, both ON CLUSTER — mirrors the shape of the notebook's DDL cells for
// both the agg-results and anomaly-events tables, driven by the columns/
// order-by/partition/ttl/sharding-key the config carries rather than a
// hardcoded schema, since this now backs two different table shapes.
@Slf4j
public class ClickHouseDdlInitializer {

    private ClickHouseDdlInitializer() {}

    public static void initialize(ClickHouseSinkConfig config) {
        log.info("Initializing ClickHouse DDL: local={}.{} distributed={}.{}",
                config.getLocalDatabase(), config.getLocalTable(), config.getDatabase(), config.getTable());

        HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
        String hostUrl = config.getFirstHostUrl();
        String clusterClause = " ON CLUSTER " + config.getClusterName();

        executeDdl(client, hostUrl, config, "CREATE DATABASE IF NOT EXISTS " + config.getLocalDatabase() + clusterClause);
        executeDdl(client, hostUrl, config, "CREATE DATABASE IF NOT EXISTS " + config.getDatabase() + clusterClause);

        // ClickHouse resolves {shard}/{replica} server-side via its own macros
        // config — these are literal tokens, not Java string substitutions.
        String zkPath = "/clickhouse/tables/{shard}/" + config.getLocalDatabase() + "/" + config.getLocalTable();
        boolean hasVersion = config.getVersionColumn() != null && !config.getVersionColumn().isEmpty();
        String engine = String.format("ReplicatedReplacingMergeTree('%s', '{replica}'%s)",
                zkPath, hasVersion ? ", " + config.getVersionColumn() : "");

        String createLocalTable = String.format("""
                CREATE TABLE IF NOT EXISTS %s.%s %s
                (
                    %s
                )
                ENGINE = %s
                PARTITION BY %s
                ORDER BY %s
                TTL %s
                SETTINGS index_granularity = 8192
                """,
                config.getLocalDatabase(), config.getLocalTable(), clusterClause,
                config.getColumnsDdl(), engine, config.getPartitionByExpr(), config.getOrderByColumns(), config.getTtlExpr());
        executeDdl(client, hostUrl, config, createLocalTable);

        String createDistributed = String.format("""
                CREATE TABLE IF NOT EXISTS %s.%s %s AS %s.%s
                ENGINE = Distributed(%s, %s, %s, %s)
                """,
                config.getDatabase(), config.getTable(), clusterClause,
                config.getLocalDatabase(), config.getLocalTable(),
                config.getClusterName(), config.getLocalDatabase(), config.getLocalTable(), config.getShardingKeyExpr());
        executeDdl(client, hostUrl, config, createDistributed);
    }

    private static void executeDdl(HttpClient client, String hostUrl, ClickHouseSinkConfig config, String query) {
        try {
            HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                    .uri(URI.create(hostUrl + "/"))
                    .timeout(Duration.ofSeconds(30))
                    .header("X-ClickHouse-User", config.getUser())
                    .POST(HttpRequest.BodyPublishers.ofString(query));
            if (config.getPassword() != null && !config.getPassword().isEmpty()) {
                requestBuilder.header("X-ClickHouse-Key", config.getPassword());
            }
            HttpRequest request = requestBuilder.build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                log.error("ClickHouse DDL failed ({}): {}\nQuery: {}", response.statusCode(), response.body(), query);
                throw new RuntimeException("ClickHouse DDL initialization failed");
            }
        } catch (Exception e) {
            log.error("Failed to execute ClickHouse DDL", e);
            throw new RuntimeException(e);
        }
    }
}
