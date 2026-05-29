package in.gov.uidai.dp.velocity.engine.config;

import lombok.extern.slf4j.Slf4j;

/**
 * Validates a {@link JobConfig} on startup and fails fast with a clear error message.
 */
@Slf4j
public class ConfigValidator {

    private ConfigValidator() {}

    public static void validate(JobConfig cfg) {
        notBlank(cfg.getJobName(),  "job-name");
        notBlank(cfg.getCluster(),  "cluster");
        notBlank(cfg.getDc(),       "dc");
        check(cfg.getParallelism() > 0, "parallelism must be > 0, got: " + cfg.getParallelism());
        check(cfg.getSources() != null && !cfg.getSources().isEmpty(), "sources must not be empty");
        check(cfg.getKafka() != null, "kafka config is required");
        notBlank(cfg.getKafka().getBrokers(),    "kafka.brokers");
        notBlank(cfg.getKafka().getRulesTopic(), "kafka.rules-topic");
        notBlank(cfg.getKafka().getRulesGroupId(), "kafka.rules-group-id");

        for (SourceTopicConfig src : cfg.getSources()) {
            notBlank(src.getTopic(),   "sources[].topic");
            notBlank(src.getGroupId(), "sources[].group-id");
        }

        check(cfg.getCheckpoint() != null, "checkpoint config is required");
        notBlank(cfg.getCheckpoint().getStorage(), "checkpoint.storage");
        check(cfg.getCheckpoint().getStorage().startsWith("s3a://")
                || cfg.getCheckpoint().getStorage().startsWith("s3://"),
                "checkpoint.storage must start with s3a:// or s3://");

        if (cfg.getSinks().getClickhouse().isEnabled()) {
            notBlank(cfg.getSinks().getClickhouse().getDatabase(), "sinks.clickhouse.database");
            check(cfg.getSinks().getClickhouse().getHosts() != null
                    && !cfg.getSinks().getClickhouse().getHosts().isEmpty(),
                    "sinks.clickhouse.hosts must not be empty");
        }

        if (cfg.getSinks().getKeydb().isEnabled()) {
            notBlank(cfg.getSinks().getKeydb().getHosts(), "sinks.keydb.hosts");
        }

        if (cfg.getPostgres() != null) {
            notBlank(cfg.getPostgres().getHost(), "postgres.host");
        }

        log.info("Config validated OK — job={} cluster={} dc={} parallelism={} sources={}",
                cfg.getJobName(), cfg.getCluster(), cfg.getDc(),
                cfg.getParallelism(), cfg.getSources().stream()
                        .map(SourceTopicConfig::getTopic).toList());
    }

    private static void notBlank(String val, String field) {
        check(val != null && !val.isBlank(), "Required config field '" + field + "' is blank or missing");
    }

    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException("Invalid jobconf.yaml: " + message);
        }
    }
}