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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
        private final long flushIntervalMs;

        private final List<AggregationResult> buffer;
        private final HttpClient httpClient;
        private final ExecutorService executor;
        private long lastFlushTime;

        public ClickHouseSinkWriter(ClickHouseSinkConfig config) {
            this.hostUrl = config.getFirstHostUrl();
            this.user = config.getUser();
            this.password = config.getPassword();
            this.database = config.getDatabase();
            this.table = config.getTable();
            this.batchSize = config.getMaxBufferSize();
            this.flushIntervalMs = config.getFlushIntervalMs();

            this.buffer = new ArrayList<>(Math.min(batchSize, 100));
            this.lastFlushTime = System.currentTimeMillis();
            this.executor = Executors.newFixedThreadPool(2);
            this.httpClient = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(10))
                    .executor(executor)
                    .build();

            log.info("ClickHouseSinkWriter initialized: host={}, db={}, table={}, batchSize={}, flushIntervalMs={}",
                    hostUrl, database, table, batchSize, flushIntervalMs);
        }

        @Override
        public void write(AggregationResult value, Context context) {
            buffer.add(value);
            log.info("Buffered AggregationResult for rule={}, groupKey={}, buffer size={}",
                    value.getId(), value.getEntityValue(), buffer.size());

            long now = System.currentTimeMillis();
            if (buffer.size() >= batchSize || (now - lastFlushTime) >= flushIntervalMs) {
                doFlush();
            }
        }

        @Override
        public void flush(boolean endOfInput) {
            if (!buffer.isEmpty()) {
                doFlush();
            }
        }


        private void doFlush() {
            if (buffer.isEmpty()) return;

            List<AggregationResult> toFlush = new ArrayList<>(buffer);
            buffer.clear();
            lastFlushTime = System.currentTimeMillis();

            StringBuilder payload = new StringBuilder();
            for (AggregationResult r : toFlush) {
                String json = ClickHouseResultConverter.toJsonEachRow(r);
                if (json != null) {
                    payload.append(json).append("\n");
                }
            }

            if (payload.length() == 0) return;

            String query = "INSERT INTO " + table + " FORMAT JSONEachRow";
            String encodedQuery = java.net.URLEncoder.encode(query, java.nio.charset.StandardCharsets.UTF_8);
            String url = String.format("%s/?database=%s&query=%s", hostUrl, database, encodedQuery);

            log.info("Flushing {} records to ClickHouse: {}.{}", toFlush.size(), database, table);

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofSeconds(30))
                    .header("Content-Type", "application/json")
                    .header("X-ClickHouse-User", user)
                    .header("X-ClickHouse-Key", password)
                    .POST(HttpRequest.BodyPublishers.ofString(payload.toString()))
                    .build();

            int maxRetries = 5;
            long[] backoffMs = {500, 1000, 2000, 4000, 8000};

            for (int i = 0; i < maxRetries; i++) {
                try {
                    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                    if (response.statusCode() == 200) {
                        log.info("Successfully wrote {} records to ClickHouse", toFlush.size());
                        return;
                    }
                    log.error("ClickHouse insert failed ({}): body={}",
                            response.statusCode(), response.body());
                    if (response.statusCode() == 400) {
                        log.error("ClickHouse 400 Bad Request — dropping batch of {} records (schema/data error)", toFlush.size());
                        return;
                    }
                    if (i == maxRetries - 1) {
                        log.error("ClickHouse insert failed after {} retries — dropping batch of {} records", maxRetries, toFlush.size());
                        return;
                    }
                    log.warn("Retrying {}/{}...", i + 1, maxRetries);
                } catch (Exception ex) {
                    if (i == maxRetries - 1) {
                        log.error("ClickHouse request failed after {} retries — dropping batch of {} records", maxRetries, toFlush.size(), ex);
                        return;
                    }
                    log.warn("ClickHouse request failed. Retrying {}/{}...", i + 1, maxRetries, ex);
                }

                try {
                    Thread.sleep(backoffMs[i]);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.warn("Interrupted during backoff — dropping batch of {} records", toFlush.size());
                    return;
                }
            }
        }

        @Override
        public void close() {
            flush(true);
            executor.shutdown();
            try {
                if (!executor.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }
}