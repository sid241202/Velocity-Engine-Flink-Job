package in.gov.uidai.dp.velocity.engine.config;

import lombok.Builder;
import lombok.Value;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Value
@Builder
public class RedisConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    public enum Mode { STANDALONE, CLUSTER }

    Mode mode;
    List<HostPort> hosts;
    String password;
    int maxTotal;
    int maxIdle;
    int minIdle;
    int timeoutMs;
    int connectTimeoutMs;
    int maxAttempts;

    @Value
    public static class HostPort implements Serializable {
        private static final long serialVersionUID = 1L;
        String host;
        int port;
    }

    public static RedisConfig fromConfig() {
        Mode mode = "CLUSTER".equalsIgnoreCase(AuthDemoConfig.REDIS_MODE) ? Mode.CLUSTER : Mode.STANDALONE;
        List<HostPort> hosts = Arrays.stream(AuthDemoConfig.REDIS_HOSTS.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .map(s -> {
                    String[] parts = s.split(":");
                    if (parts.length != 2) throw new IllegalArgumentException("Invalid Redis host:port: " + s);
                    return new HostPort(parts[0].trim(), Integer.parseInt(parts[1].trim()));
                })
                .collect(Collectors.toList());
        return RedisConfig.builder()
                .mode(mode).hosts(hosts)
                .password(AuthDemoConfig.REDIS_PASSWORD)
                .maxTotal(AuthDemoConfig.REDIS_MAX_TOTAL)
                .maxIdle(AuthDemoConfig.REDIS_MAX_IDLE)
                .minIdle(AuthDemoConfig.REDIS_MIN_IDLE)
                .timeoutMs(AuthDemoConfig.REDIS_TIMEOUT_MS)
                .connectTimeoutMs(AuthDemoConfig.REDIS_CONNECT_TIMEOUT_MS)
                .maxAttempts(AuthDemoConfig.REDIS_MAX_ATTEMPTS)
                .build();
    }

    public HostPort firstHost() {
        if (hosts == null || hosts.isEmpty()) throw new IllegalStateException("No Redis hosts configured");
        return hosts.get(0);
    }
}
