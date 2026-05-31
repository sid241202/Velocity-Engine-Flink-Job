package in.gov.uidai.dp.velocity.engine.config;

import java.io.Serializable;
import java.util.List;

//TODO
public class ClickHouseSinkConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    private boolean enabled = true;
    private List<String> hosts;
    private String user = "default";
    private String password;
    private String clusterName;
    private boolean useDistributed = false;
    private String zookeeperPath;
    private String database;
    private String table = "rule_results";
    private boolean autoCreateDdl = true;
    private int numWriters = 4;
    private int queueMaxCapacity = 10000;
    private int maxBufferSize = 5000;
    private long flushIntervalMs = 5000L;
    private int timeoutSec = 60;
    private int numRetries = 3;
    private String failedRecordsPath = "/opt/flink/failed_records";

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public List<String> getHosts() {
        return hosts;
    }

    public void setHosts(List<String> hosts) {
        this.hosts = hosts;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public boolean isUseDistributed() {
        return useDistributed;
    }

    public void setUseDistributed(boolean useDistributed) {
        this.useDistributed = useDistributed;
    }

    public String getZookeeperPath() {
        return zookeeperPath;
    }

    public void setZookeeperPath(String zookeeperPath) {
        this.zookeeperPath = zookeeperPath;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public boolean isAutoCreateDdl() {
        return autoCreateDdl;
    }

    public void setAutoCreateDdl(boolean autoCreateDdl) {
        this.autoCreateDdl = autoCreateDdl;
    }

    public int getNumWriters() {
        return numWriters;
    }

    public void setNumWriters(int numWriters) {
        this.numWriters = numWriters;
    }

    public int getQueueMaxCapacity() {
        return queueMaxCapacity;
    }

    public void setQueueMaxCapacity(int queueMaxCapacity) {
        this.queueMaxCapacity = queueMaxCapacity;
    }

    public int getMaxBufferSize() {
        return maxBufferSize;
    }

    public void setMaxBufferSize(int maxBufferSize) {
        this.maxBufferSize = maxBufferSize;
    }

    public long getFlushIntervalMs() {
        return flushIntervalMs;
    }

    public void setFlushIntervalMs(long flushIntervalMs) {
        this.flushIntervalMs = flushIntervalMs;
    }

    public int getTimeoutSec() {
        return timeoutSec;
    }

    public void setTimeoutSec(int timeoutSec) {
        this.timeoutSec = timeoutSec;
    }

    public int getNumRetries() {
        return numRetries;
    }

    public void setNumRetries(int numRetries) {
        this.numRetries = numRetries;
    }

    public String getFailedRecordsPath() {
        return failedRecordsPath;
    }

    public void setFailedRecordsPath(String failedRecordsPath) {
        this.failedRecordsPath = failedRecordsPath;
    }

    public String getFirstHost() {
        return (hosts != null && !hosts.isEmpty()) ? hosts.get(0) : null;
    }

    public String getFirstHostUrl() {
        String h = getFirstHost();
        return h != null ? "http://" + h : null;
    }
}