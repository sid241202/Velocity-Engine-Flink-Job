package in.gov.uidai.dp.velocity.engine.sinks;

import in.gov.uidai.dp.velocity.engine.config.RedisConfig;
import in.gov.uidai.dp.velocity.engine.model.AnomalyEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import redis.clients.jedis.Connection;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

@Slf4j
public class RedisSink implements Sink<AnomalyEvent> {
    private static final long serialVersionUID = 1L;

    private final RedisConfig config;
    private final int penaltyTtlSeconds;

    public RedisSink(RedisConfig config, int penaltyTtlSeconds) {
        this.config = config;
        this.penaltyTtlSeconds = penaltyTtlSeconds;
    }

    @Override
    public SinkWriter<AnomalyEvent> createWriter(WriterInitContext context) {
        return new RedisWriter(config, penaltyTtlSeconds);
    }

    @Slf4j
    public static class RedisWriter implements SinkWriter<AnomalyEvent> {
        private final RedisConfig config;
        private final int penaltyTtlSeconds;
        private final JedisPool jedisPool;
        private final JedisCluster jedisCluster;

        public RedisWriter(RedisConfig config, int penaltyTtlSeconds) {
            this.config = config;
            this.penaltyTtlSeconds = penaltyTtlSeconds;

            if (config.getMode() == RedisConfig.Mode.CLUSTER) {
                GenericObjectPoolConfig<Connection> clusterPoolConfig = new GenericObjectPoolConfig<>();
                clusterPoolConfig.setMaxTotal(config.getMaxTotal());
                clusterPoolConfig.setMaxIdle(config.getMaxIdle());
                clusterPoolConfig.setMinIdle(config.getMinIdle());
                clusterPoolConfig.setTestOnBorrow(true);
                clusterPoolConfig.setTestOnReturn(true);
                clusterPoolConfig.setTestWhileIdle(true);
                clusterPoolConfig.setMinEvictableIdleDuration(Duration.ofSeconds(60));
                clusterPoolConfig.setTimeBetweenEvictionRuns(Duration.ofSeconds(30));
                clusterPoolConfig.setNumTestsPerEvictionRun(3);
                clusterPoolConfig.setBlockWhenExhausted(true);

                Set<HostAndPort> nodes = new HashSet<>();
                for (RedisConfig.HostPort hp : config.getHosts()) {
                    nodes.add(new HostAndPort(hp.getHost(), hp.getPort()));
                }
                String pw = (config.getPassword() != null && !config.getPassword().isEmpty()) ? config.getPassword() : null;
                this.jedisCluster = new JedisCluster(nodes, config.getConnectTimeoutMs(), config.getTimeoutMs(),
                        config.getMaxAttempts(), pw, clusterPoolConfig);
                this.jedisPool = null;
                log.info("RedisSink CLUSTER mode, {} nodes", nodes.size());
            } else {
                JedisPoolConfig poolConfig = new JedisPoolConfig();
                poolConfig.setMaxTotal(config.getMaxTotal());
                poolConfig.setMaxIdle(config.getMaxIdle());
                poolConfig.setMinIdle(config.getMinIdle());
                poolConfig.setTestOnBorrow(true);
                poolConfig.setTestOnReturn(true);
                poolConfig.setTestWhileIdle(true);
                poolConfig.setMinEvictableIdleDuration(Duration.ofSeconds(60));
                poolConfig.setTimeBetweenEvictionRuns(Duration.ofSeconds(30));
                poolConfig.setNumTestsPerEvictionRun(3);
                poolConfig.setBlockWhenExhausted(true);

                RedisConfig.HostPort hp = config.firstHost();
                String pw = (config.getPassword() != null && !config.getPassword().isEmpty()) ? config.getPassword() : null;
                this.jedisPool = new JedisPool(poolConfig, hp.getHost(), hp.getPort(), config.getTimeoutMs(), pw);
                this.jedisCluster = null;
                log.info("RedisSink STANDALONE mode: {}:{}", hp.getHost(), hp.getPort());
            }
        }

        @Override
        public void write(AnomalyEvent event, Context context) {
            if (event == null || event.getId() == null || event.getEntityValue() == null) return;
            // Use per-event TTL (from rule_metadata.penalty_ttl_seconds); fall back to sink-level default
            int effectiveTtl = event.getPenaltyTtlSeconds() > 0 ? event.getPenaltyTtlSeconds() : penaltyTtlSeconds;
            long[] backoffMs = {200, 500, 1000};
            String key = event.getId();
            String val = event.getEntityValue();
            for (int i = 0; i < 3; i++) {
                try {
                    if (config.getMode() == RedisConfig.Mode.CLUSTER) {
                        // JedisCluster does not support pipelining across slots; execute atomically per-command
                        // but wrap in a single round-trip using MULTI/EXEC-style pipeline on the cluster node.
                        // Since SADD+EXPIRE on the same key always hits the same slot, use a pipeline via eval.
                        String luaScript = "redis.call('SADD', KEYS[1], ARGV[1]); if tonumber(ARGV[2]) > 0 then redis.call('EXPIRE', KEYS[1], ARGV[2]) end; return 1";
                        jedisCluster.eval(luaScript, 1, key, val, String.valueOf(effectiveTtl));
                    } else {
                        try (Jedis j = jedisPool.getResource()) {
                            // Use a pipeline for atomic SADD+EXPIRE in standalone mode
                            redis.clients.jedis.Pipeline pipe = j.pipelined();
                            pipe.sadd(key, val);
                            if (effectiveTtl > 0) pipe.expire(key, effectiveTtl);
                            pipe.sync();
                        }
                    }
                    log.info("Redis SADD+EXPIRE key={} val={} ttl={}s", key, val, effectiveTtl);
                    return;
                } catch (Exception e) {
                    if (i == 2) log.error("RedisSink failed after 3 attempts key={}: {}", key, e.getMessage());
                    else log.warn("RedisSink attempt {}/3 failed: {}", i + 1, e.getMessage());
                    try {
                        if (i < 2) Thread.sleep(backoffMs[i]);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }
        }

        @Override
        public void flush(boolean endOfInput) {
            // Redis writes are synchronous; nothing to flush
        }

        @Override
        public void close() {
            try { if (jedisPool != null && !jedisPool.isClosed()) jedisPool.close(); } catch (Exception e) { log.warn("JedisPool close error: {}", e.getMessage()); }
            try { if (jedisCluster != null) jedisCluster.close(); } catch (Exception e) { log.warn("JedisCluster close error: {}", e.getMessage()); }
        }
    }
}
