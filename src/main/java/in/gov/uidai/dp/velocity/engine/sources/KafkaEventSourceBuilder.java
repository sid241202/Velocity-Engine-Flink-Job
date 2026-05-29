package in.gov.uidai.dp.velocity.engine.sources;

import in.gov.uidai.dp.velocity.engine.config.JobConfig;
import in.gov.uidai.dp.velocity.engine.config.SourceTopicConfig;
import in.gov.uidai.dp.velocity.engine.deserializers.EventDeserializer;
import in.gov.uidai.dp.velocity.engine.model.Event;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.util.Properties;

/**
 * Builds a {@link KafkaSource} for a single source topic.
 * One source per topic entry in jobconf.yaml sources list.
 */
public final class KafkaEventSourceBuilder {

    private KafkaEventSourceBuilder() {}

    public static KafkaSource<Event> build(SourceTopicConfig src, JobConfig config) {
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", config.getKafka().getBrokers());
        kafkaProps.setProperty("group.id", src.getGroupId());
        kafkaProps.setProperty("enable.auto.commit", "false");
        kafkaProps.setProperty("max.poll.records", "500");
        kafkaProps.setProperty("fetch.max.bytes", "52428800");   // 50MB max fetch
        kafkaProps.setProperty("max.partition.fetch.bytes", "10485760"); // 10MB per partition

        OffsetsInitializer offsetsInit = "earliest".equalsIgnoreCase(src.getOffset())
                ? OffsetsInitializer.earliest()
                : OffsetsInitializer.latest();

        EventDeserializer deserializer = new EventDeserializer(
                src.getTopic(),
                config.getCluster(),
                src.getEventTimestampField(),
                src.getEventTimestampFormat()
        );

        return KafkaSource.<Event>builder()
                .setBootstrapServers(config.getKafka().getBrokers())
                .setTopics(src.getTopic())
                .setGroupId(src.getGroupId())
                .setStartingOffsets(offsetsInit)
                .setDeserializer(deserializer)
                .setProperties(kafkaProps)
                .build();
    }
}