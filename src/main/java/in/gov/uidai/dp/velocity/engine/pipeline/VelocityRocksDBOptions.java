package in.gov.uidai.dp.velocity.engine.pipeline;

import in.gov.uidai.dp.velocity.engine.config.RocksDBConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class VelocityRocksDBOptions {

    private VelocityRocksDBOptions() {}

    public static void apply(StreamExecutionEnvironment env, RocksDBConfig config) {
        Configuration conf = new Configuration();

        // Use RocksDB optimized settings for high-throughput incremental state
        conf.set(RocksDBOptions.TIMER_SERVICE_FACTORY, RocksDBOptions.TimerServiceFactory.ROCKSDB);

        // Memory management
        // Flink's managed memory will automatically configure the block cache and write buffers.
        // We just pass the ratios through if we want to override default behavior.
        conf.set(RocksDBOptions.USE_MANAGED_MEMORY, true);

        // Not setting specific memory options manually since USE_MANAGED_MEMORY = true
        // will let Flink handle the limits based on TaskManager managed memory size.

        env.configure(conf);
    }
}