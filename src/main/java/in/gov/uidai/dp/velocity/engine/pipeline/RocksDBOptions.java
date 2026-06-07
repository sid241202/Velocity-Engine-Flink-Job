package in.gov.uidai.dp.velocity.engine.pipeline;

import org.apache.flink.contrib.streaming.state.RocksDBOptionsFactory;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;

import java.util.Collection;

public class RocksDBOptions implements RocksDBOptionsFactory {

    private static final long serialVersionUID = 1L;

    @Override
    public DBOptions createDBOptions(DBOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
        currentOptions.setIncreaseParallelism(4);
        return currentOptions;
    }

    @Override
    public ColumnFamilyOptions createColumnOptions(ColumnFamilyOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
        currentOptions.setLevel0FileNumCompactionTrigger(4);
        currentOptions.setCompactionStyle(CompactionStyle.LEVEL);
        currentOptions.setCompressionType(CompressionType.LZ4_COMPRESSION);
        currentOptions.setBottommostCompressionType(CompressionType.ZSTD_COMPRESSION);
        currentOptions.setWriteBufferSize(64 * 1024 * 1024);
        currentOptions.setTargetFileSizeBase(64 * 1024 * 1024);
        return currentOptions;
    }
}