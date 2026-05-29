package in.gov.uidai.dp.velocity.engine.sources;

import in.gov.uidai.dp.velocity.engine.config.JobConfig;
import in.gov.uidai.dp.velocity.engine.deserializers.RuleDeserializer;
import in.gov.uidai.dp.velocity.engine.model.VelocityRule;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;

/**
 * Builds the Kafka source for the rules topic (live rule updates).
 * Rules are consumed from latest — all historical rules are bootstrapped
 * from PostgreSQL by {@link PostgresRulesBootstrapSource}.
 */
public final class KafkaRulesSourceBuilder {

    private KafkaRulesSourceBuilder() {}

    public static DataStream<VelocityRule> build(StreamExecutionEnvironment env, JobConfig config) {
        KafkaSource<String> rawSource = KafkaSource.<String>builder()
                .setBootstrapServers(config.getKafka().getBrokers())
                .setTopics(config.getKafka().getRulesTopic())
                .setGroupId(config.getKafka().getRulesGroupId())
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        return env.fromSource(rawSource, WatermarkStrategy.noWatermarks(), "Kafka-Rules-Live")
                .flatMap(new RuleDeserializer())
                .name("RuleDeserializer-Kafka")
                .uid("rule-deserializer-kafka");
    }
}