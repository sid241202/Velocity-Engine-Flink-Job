package in.gov.uidai.dp.velocity.engine.sinks;

import in.gov.uidai.dp.velocity.engine.config.ClickHouseSinkConfig;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

@Slf4j
public class ClickHouseDdlInitializer {

    private ClickHouseDdlInitializer() {}

    public static void initialize(ClickHouseSinkConfig config) {
        log.info("Initializing ClickHouse DDL for database={} table={}", config.getDatabase(), config.getTable());

        HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
        String hostUrl = config.getFirstHostUrl();
        String clusterClause = config.isUseDistributed() ? " ON CLUSTER " + config.getClusterName() : "";

        String createDb = "CREATE DATABASE IF NOT EXISTS " + config.getDatabase() + clusterClause;
        executeDdl(client, hostUrl, config, createDb);

        String localTable = config.isUseDistributed() ? config.getTable() + "_local" : config.getTable();
        String engine = config.isUseDistributed()
                ? String.format("ReplicatedReplacingMergeTree('%s/{shard}', '{replica}', evaluatedAt)", config.getZookeeperPath())
                : "ReplacingMergeTree(evaluatedAt)";

        String createLocalTable = String.format("""
            CREATE TABLE IF NOT EXISTS %s.%s %s (
                ruleId String,
                ruleName String,
                sourceTopic String,
                cluster String,
                entityName String,
                groupKey String,
                windowStart DateTime64(3, 'Asia/Kolkata'),
                windowEnd DateTime64(3, 'Asia/Kolkata'),
                windowType String,
                timeType String,
                aggregationResults String,
                thresholdBreached UInt8,
                severityLevel String,
                eventCount UInt64,
                evaluatedAt DateTime64(3, 'Asia/Kolkata')
            ) ENGINE = %s
            ORDER BY (ruleId, windowStart, groupKey)
            """, config.getDatabase(), localTable, clusterClause, engine);

        executeDdl(client, hostUrl, config, createLocalTable);

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