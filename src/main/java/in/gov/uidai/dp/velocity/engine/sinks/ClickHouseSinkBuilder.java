package in.gov.uidai.dp.velocity.engine.sinks;

import in.gov.uidai.dp.velocity.engine.config.ClickHouseSinkConfig;
import in.gov.uidai.dp.velocity.engine.model.AggregationResult;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Flink Sink for ClickHouse.
 *
 * <p>Since official ClickHouse JDBC connectors often struggle with Flink 2.2.0 compatibility,
 * this implements an asynchronous HTTP-based batch sink using Java 11+ HttpClient.
 * It batches {@link AggregationResult} records and flushes them to the ClickHouse HTTP API
 * using JSONEachRow format.
 */
@Slf4j
public class ClickHouseSinkBuilder {

    private ClickHouseSinkBuilder() {}

    public static Sink<AggregationResult> build(ClickHouseSinkConfig config) {
        if (config.isAutoCreateDdl()) {
            ClickHouseDdlInitializer.initialize(config);
        }
        return new AsyncClickHouseHttpSink(config);
    }

    public static class AsyncClickHouseHttpSink implements Sink<AggregationResult> {
        private static final long serialVersionUID = 1L;

        private final ClickHouseSinkConfig config;

        public AsyncClickHouseHttpSink(ClickHouseSinkConfig config) {
            this.config = config;
        }

        @Override
        public SinkWriter<AggregationResult> createWriter(WriterInitContext context) {
            return new ClickHouseSinkWriter(config);
        }
    }

    public static class ClickHouseSinkWriter implements SinkWriter<AggregationResult> {
        private final String hostUrl;
        private final String user;
        private final String password;
        private final String database;
        private final String table;
        private final int batchSize;

        private final List<AggregationResult> buffer;
        private final HttpClient httpClient;
        private final ExecutorService executor;

        public ClickHouseSinkWriter(ClickHouseSinkConfig config) {
            this.hostUrl = config.getFirstHostUrl();
            this.user = config.getUser();
            this.password = config.getPassword();
            this.database = config.getDatabase();
            this.table = config.getTable();
            this.batchSize = config.getMaxBufferSize();

            this.buffer = new ArrayList<>(batchSize);
            this.executor = Executors.newFixedThreadPool(2);
            this.httpClient = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(10))
                    .executor(executor)
                    .build();
        }

        @Override
        public void write(AggregationResult value, Context context) {
            buffer.add(value);
            if (buffer.size() >= batchSize) {
                flush(false);
            }
        }

        @Override
        public void flush(boolean endOfInput) {
            if (buffer.isEmpty()) return;

            List<AggregationResult> toFlush = new ArrayList<>(buffer);
            buffer.clear();

            StringBuilder payload = new StringBuilder();
            for (AggregationResult r : toFlush) {
                payload.append(ClickHouseResultConverter.toJsonEachRow(r)).append("\n");
            }

            String url = String.format("%s/?database=%s&query=INSERT INTO %s FORMAT JSONEachRow",
                    hostUrl, database, table);

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofSeconds(30))
                    .header("Content-Type", "application/json")
                    .header("X-ClickHouse-User", user)
                    .header("X-ClickHouse-Key", password)
                    .POST(HttpRequest.BodyPublishers.ofString(payload.toString()))
                    .build();

            try {
                HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() != 200) {
                    log.error("ClickHouse insert failed ({}): {}", response.statusCode(), response.body());
                }
            } catch (Exception ex) {
                log.error("ClickHouse request failed", ex);
            }
        }

        @Override
        public void close() {
            flush(true);
            executor.shutdown();
        }
    }
}