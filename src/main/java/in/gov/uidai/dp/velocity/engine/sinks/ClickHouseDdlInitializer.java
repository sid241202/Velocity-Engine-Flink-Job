package in.gov.uidai.dp.velocity.engine.sinks;

import in.gov.uidai.dp.velocity.engine.config.ClickHouseSinkConfig;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/**
 * Executes CREATE DATABASE and CREATE TABLE on ClickHouse via HTTP if enabled.
 * Creates local and distributed tables if use-distributed is true.
 */
@Slf4j
public class ClickHouseDdlInitializer {

    private ClickHouseDdlInitializer() {}

    public static void initialize(ClickHouseSinkConfig config) {
        log.info("Initializing ClickHouse DDL for database={} table={}", config.getDatabase(), config.getTable());

        HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
        String hostUrl = config.getFirstHostUrl();
        String clusterClause = config.isUseDistributed() ? " ON CLUSTER " + config.getClusterName() : "";

        // 1. Create Database
        String createDb = "CREATE DATABASE IF NOT EXISTS " + config.getDatabase() + clusterClause;
        executeDdl(client, hostUrl, config, createDb);

        // 2. Create Local Table (ReplicatedMergeTree)
        String localTable = config.isUseDistributed() ? config.getTable() + "_local" : config.getTable();
        String engine = config.isUseDistributed()
                ? String.format("ReplicatedMergeTree('%s/{shard}', '{replica}')", config.getZookeeperPath())
                : "MergeTree()";

        String createLocalTable = String.format("""
            CREATE TABLE IF NOT EXISTS %s.%s %s (
                ruleId String,
                ruleName String,
                sourceTopic String,
                cluster String,
                groupKey String,
                windowStart String,
                windowEnd String,
                windowType String,
                timeType String,
                aggregationResults String,
                thresholdBreached UInt8,
                severityLevel String,
                eventCount UInt64,
                evaluatedAt String
            ) ENGINE = %s
            ORDER BY (ruleId, windowStart, groupKey)
            """, config.getDatabase(), localTable, clusterClause, engine);

        executeDdl(client, hostUrl, config, createLocalTable);

        // 3. Create Distributed Table if requested
        if (config.isUseDistributed()) {
            String createDistributed = String.format("""
                CREATE TABLE IF NOT EXISTS %s.%s %s AS %s.%s
                ENGINE = Distributed(%s, %s, %s, rand())
                """,
                config.getDatabase(), config.getTable(), clusterClause,
                config.getDatabase(), localTable,
                config.getClusterName(), config.getDatabase(), localTable);
            executeDdl(client, hostUrl, config, createDistributed);
        }
    }

    private static void executeDdl(HttpClient client, String hostUrl, ClickHouseSinkConfig config, String query) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(hostUrl + "/"))
                    .timeout(Duration.ofSeconds(30))
                    .header("X-ClickHouse-User", config.getUser())
                    .header("X-ClickHouse-Key", config.getPassword())
                    .POST(HttpRequest.BodyPublishers.ofString(query))
                    .build();

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