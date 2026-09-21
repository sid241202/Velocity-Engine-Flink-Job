package in.gov.uidai.dp.velocity.engine.sinks;

import com.codahale.metrics.SlidingWindowReservoir;
import in.gov.uidai.dp.velocity.engine.config.ClickHouseSinkConfig;
import in.gov.uidai.dp.velocity.engine.utils.TimeUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MetricGroup;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

// Generic buffered/batched HTTP sink into a ClickHouse Distributed table.
// Shared by the agg-results and anomaly-events writers (see
// AuthDemoPipeline) rather than duplicated, since both need identical
// batching/retry/backoff/metrics behavior against two different row shapes —
// parameterized here by a row-JSON mapper and a producedAt extractor (for the
// write-delay metric), following the same metricGroup pattern KeyDbSink uses.
@Slf4j
public final class ClickHouseSinkBuilder {

    private ClickHouseSinkBuilder() {}

    public static <T> Sink<T> build(ClickHouseSinkConfig config, Function<T, String> rowMapper,
            Function<T, String> producedAtExtractor, String metricPrefix) {
        if (config.isAutoCreateDdl()) {
            ClickHouseDdlInitializer.initialize(config);
        }
        return new AsyncClickHouseHttpSink<>(config, rowMapper, producedAtExtractor, metricPrefix);
    }

    public static class AsyncClickHouseHttpSink<T> implements Sink<T> {
        private static final long serialVersionUID = 1L;

        private final ClickHouseSinkConfig config;
        private final Function<T, String> rowMapper;
        private final Function<T, String> producedAtExtractor;
        private final String metricPrefix;

        public AsyncClickHouseHttpSink(ClickHouseSinkConfig config, Function<T, String> rowMapper,
                Function<T, String> producedAtExtractor, String metricPrefix) {
            this.config = config;
            this.rowMapper = rowMapper;
            this.producedAtExtractor = producedAtExtractor;
            this.metricPrefix = metricPrefix;
        }

        @Override
        public SinkWriter<T> createWriter(WriterInitContext context) {
            return new ClickHouseSinkWriter<>(config, rowMapper, producedAtExtractor, metricPrefix, context.metricGroup());
        }
    }

    public static class ClickHouseSinkWriter<T> implements SinkWriter<T> {
        private final String hostUrl;
        private final String user;
        private final String password;
        private final String database;
        private final String table;
        private final int batchSize;
        private final long flushIntervalMs;
        private final Function<T, String> rowMapper;
        private final Function<T, String> producedAtExtractor;

        private final List<T> buffer;
        private final HttpClient httpClient;
        private final ExecutorService executor;
        private long lastFlushTime;

        // Metric names follow the velocity_clickhouse_<prefix>_write_* scheme —
        // "agg" and "anomaly" prefixes for the two current tables — matching the
        // velocity_redis_write_* naming already established in KeyDbSink.
        private final Histogram writeDurationMsHistogram;
        private final Histogram writeDelayMsHistogram;
        private final Counter writeErrorsCounter;
        private final Counter rowsWrittenCounter;

        public ClickHouseSinkWriter(ClickHouseSinkConfig config, Function<T, String> rowMapper,
                Function<T, String> producedAtExtractor, String metricPrefix, MetricGroup metricGroup) {
            this.hostUrl = config.getFirstHostUrl();
            this.user = config.getUser();
            this.password = config.getPassword();
            this.database = config.getDatabase();
            this.table = config.getTable();
            this.batchSize = config.getMaxBufferSize();
            this.flushIntervalMs = config.getFlushIntervalMs();
            this.rowMapper = rowMapper;
            this.producedAtExtractor = producedAtExtractor;

            this.buffer = new ArrayList<>(Math.min(batchSize, 100));
            this.lastFlushTime = System.currentTimeMillis();
            this.executor = Executors.newFixedThreadPool(2);
            this.httpClient = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(10))
                    .executor(executor)
                    .build();

            String prefix = "velocity_clickhouse_" + metricPrefix;
            this.writeDurationMsHistogram = metricGroup.histogram(prefix + "_write_duration_ms",
                    new DropwizardHistogramWrapper(new com.codahale.metrics.Histogram(new SlidingWindowReservoir(500))));
            this.writeDelayMsHistogram = metricGroup.histogram(prefix + "_write_delay_ms",
                    new DropwizardHistogramWrapper(new com.codahale.metrics.Histogram(new SlidingWindowReservoir(500))));
            this.writeErrorsCounter = metricGroup.counter(prefix + "_write_errors_total");
            this.rowsWrittenCounter = metricGroup.counter(prefix + "_rows_written_total");

            log.info("ClickHouseSinkWriter[{}] initialized: host={}, db={}, table={}, batchSize={}, flushIntervalMs={}",
                    metricPrefix, hostUrl, database, table, batchSize, flushIntervalMs);
        }

        @Override
        public void write(T value, Context context) throws IOException {
            buffer.add(value);

            long now = System.currentTimeMillis();
            if (buffer.size() >= batchSize || (now - lastFlushTime) >= flushIntervalMs) {
                doFlush();
            }
        }

        @Override
        public void flush(boolean endOfInput) throws IOException {
            if (!buffer.isEmpty()) {
                doFlush();
            }
        }

        private void doFlush() throws IOException {
            if (buffer.isEmpty()) return;

            List<T> toFlush = new ArrayList<>(buffer);
            buffer.clear();
            lastFlushTime = System.currentTimeMillis();

            StringBuilder payload = new StringBuilder();
            for (T r : toFlush) {
                String json = rowMapper.apply(r);
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
            long writeStartMs = System.currentTimeMillis();

            for (int i = 0; i < maxRetries; i++) {
                try {
                    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                    if (response.statusCode() == 200) {
                        log.info("Successfully wrote {} records to ClickHouse", toFlush.size());
                        writeDurationMsHistogram.update(System.currentTimeMillis() - writeStartMs);
                        rowsWrittenCounter.inc(toFlush.size());
                        recordDelay(toFlush);
                        return;
                    }
                    log.error("ClickHouse insert failed ({}): body={}", response.statusCode(), response.body());
                    if (response.statusCode() == 400) {
                        // Malformed request/schema mismatch — retrying identical bytes won't
                        // help. Fail loudly (see class doc) rather than silently drop.
                        writeErrorsCounter.inc();
                        throw new IOException("ClickHouse insert failed with 400 Bad Request (schema/data error), "
                                + toFlush.size() + " records — failing sink so Flink restarts from the last "
                                + "checkpoint and replays; ReplacingMergeTree dedups the retry.");
                    }
                    if (i == maxRetries - 1) {
                        writeErrorsCounter.inc();
                        throw new IOException("ClickHouse insert failed after " + maxRetries + " retries, dropping "
                                + toFlush.size() + " records would be silent data loss — failing sink instead so "
                                + "Flink restarts from the last checkpoint and replays.");
                    }
                    log.warn("Retrying {}/{}...", i + 1, maxRetries);
                } catch (IOException ioe) {
                    throw ioe;
                } catch (Exception ex) {
                    if (i == maxRetries - 1) {
                        writeErrorsCounter.inc();
                        throw new IOException("ClickHouse request failed after " + maxRetries + " retries, "
                                + toFlush.size() + " records — failing sink so Flink restarts from the last "
                                + "checkpoint and replays.", ex);
                    }
                    log.warn("ClickHouse request failed. Retrying {}/{}...", i + 1, maxRetries, ex);
                }

                try {
                    Thread.sleep(backoffMs[i]);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted during ClickHouse write backoff, "
                            + toFlush.size() + " records unflushed", e);
                }
            }
        }

        private void recordDelay(List<T> toFlush) {
            if (toFlush.isEmpty()) return;
            String producedAt = producedAtExtractor.apply(toFlush.get(toFlush.size() - 1));
            long producedAtMs = producedAt != null ? TimeUtils.istStringToEpochMs(producedAt) : -1L;
            if (producedAtMs > 0) {
                writeDelayMsHistogram.update(Math.max(0L, System.currentTimeMillis() - producedAtMs));
            }
        }

        @Override
        public void close() throws IOException {
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
