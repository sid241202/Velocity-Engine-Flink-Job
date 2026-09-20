package in.gov.uidai.dp.velocity.engine.config;

import lombok.Builder;
import lombok.Value;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Value
@Builder
public class KeyDbConfig implements Serializable {
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

    public static KeyDbConfig fromConfig() {
        Mode mode = "CLUSTER".equalsIgnoreCase(AuthDemoConfig.KEYDB_MODE) ? Mode.CLUSTER : Mode.STANDALONE;
        List<HostPort> hosts = Arrays.stream(AuthDemoConfig.KEYDB_HOSTS.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .map(s -> {
                    String[] parts = s.split(":");
                    if (parts.length != 2) throw new IllegalArgumentException("Invalid KeyDB host:port: " + s);
                    return new HostPort(parts[0].trim(), Integer.parseInt(parts[1].trim()));
                })
                .collect(Collectors.toList());
        return KeyDbConfig.builder()
                .mode(mode).hosts(hosts)
                .password(AuthDemoConfig.KEYDB_PASSWORD)
                .maxTotal(AuthDemoConfig.KEYDB_MAX_TOTAL)
                .maxIdle(AuthDemoConfig.KEYDB_MAX_IDLE)
                .minIdle(AuthDemoConfig.KEYDB_MIN_IDLE)
                .timeoutMs(AuthDemoConfig.KEYDB_TIMEOUT_MS)
                .connectTimeoutMs(AuthDemoConfig.KEYDB_CONNECT_TIMEOUT_MS)
                .maxAttempts(AuthDemoConfig.KEYDB_MAX_ATTEMPTS)
                .build();
    }

    public HostPort firstHost() {
        if (hosts == null || hosts.isEmpty()) throw new IllegalStateException("No KeyDB hosts configured");
        return hosts.get(0);
    }
}
