package in.gov.uidai.dp.velocity.engine.pipeline;

import in.gov.uidai.dp.velocity.engine.config.AuthDemoConfig;
import in.gov.uidai.dp.velocity.engine.config.ClickHouseSinkConfig;
import in.gov.uidai.dp.velocity.engine.deserializers.EventDeserializer;
import in.gov.uidai.dp.velocity.engine.deserializers.RuleDeserializer;
import in.gov.uidai.dp.velocity.engine.functions.AuthDeduplicationFunction;
import in.gov.uidai.dp.velocity.engine.functions.DynamicKeyFunction;
import in.gov.uidai.dp.velocity.engine.functions.RuleEvaluatorFunction;
import in.gov.uidai.dp.velocity.engine.model.AggregationResult;
import in.gov.uidai.dp.velocity.engine.model.Event;
import in.gov.uidai.dp.velocity.engine.model.Keyed;
import in.gov.uidai.dp.velocity.engine.model.VelocityRule;
import in.gov.uidai.dp.velocity.engine.sinks.ClickHouseSinkBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.time.Duration;
import java.util.Collections;

@Slf4j
public class AuthDemoPipeline {

    public void buildAndExecute() throws Exception {
        Configuration conf = new Configuration();
        conf.setString("state.backend.rocksdb.options-factory",
                "in.gov.uidai.dp.velocity.engine.pipeline.RocksDBOptions");
        conf.setString("state.checkpoints.dir", AuthDemoConfig.CHECKPOINT_DIR);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(9);

        env.enableCheckpointing(60000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(600000L);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10000L);
        env.getCheckpointConfig().setExternalizedCheckpointRetention(
                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().enableUnalignedCheckpoints();

        KafkaSource<Event> kafkaSource = KafkaSource.<Event>builder()
                .setBootstrapServers(AuthDemoConfig.KAFKA_BOOTSTRAP)
                .setTopics(AuthDemoConfig.AUTH_TOPIC)
                .setGroupId(AuthDemoConfig.AUTH_CONSUMER_GROUP)
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setDeserializer(new EventDeserializer(AuthDemoConfig.AUTH_TOPIC,
                        AuthDemoConfig.CLUSTER_NAME,
                        AuthDemoConfig.EVENT_TIMESTAMP_FIELD,
                        AuthDemoConfig.EVENT_TIMESTAMP_FORMAT))
                .build();

        WatermarkStrategy<Event> watermarkStrategy = WatermarkStrategy
                .<Event>forBoundedOutOfOrderness(Duration.ofMillis(AuthDemoConfig.MAX_WATERMARK_LAG_MS))
                .withIdleness(Duration.ofSeconds(30));

        DataStream<Event> eventsStream = env.fromSource(kafkaSource, watermarkStrategy, "Kafka-Auth-Events")
                .uid("kafka-auth-events");

        SingleOutputStreamOperator<Event> deduplicatedEvents = eventsStream
                .keyBy(event -> {
                    Object authCode = in.gov.uidai.dp.velocity.engine.utils.FieldExtractor
                            .extractObject(event, "_data.authCode");
                    return authCode != null ? authCode.toString() : "";
                })
                .process(new AuthDeduplicationFunction())
                .name("AuthDeduplicator")
                .uid("auth-deduplicator");

        KafkaSource<VelocityRule> rulesSource = KafkaSource.<VelocityRule>builder()
                .setBootstrapServers(AuthDemoConfig.KAFKA_BOOTSTRAP)
                .setTopics(AuthDemoConfig.RULES_TOPIC)
                .setGroupId(AuthDemoConfig.RULES_CONSUMER_GROUP)
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setDeserializer(new RuleDeserializer())
                .build();

        DataStream<VelocityRule> kafkaRules = env
                .fromSource(rulesSource, WatermarkStrategy.<VelocityRule>forMonotonousTimestamps()
                        .withIdleness(Duration.ofSeconds(30)), "Kafka-Rules")
                .uid("kafka-rules")
                .setParallelism(1);

        BroadcastStream<VelocityRule> broadcastRules = kafkaRules.broadcast(DynamicKeyFunction.RULE_STATE_DESC);

        SingleOutputStreamOperator<Keyed<Event, String, String>> keyedEvents = deduplicatedEvents
                .connect(broadcastRules)
                .process(new DynamicKeyFunction(AuthDemoConfig.CLUSTER_NAME))
                .name("DynamicKeyFunction")
                .uid("dynamic-key-function");

        SingleOutputStreamOperator<AggregationResult> results = keyedEvents
                .keyBy(keyed -> keyed.getId() + "|" + keyed.getKey())
                .connect(broadcastRules)
                .process(new RuleEvaluatorFunction(AuthDemoConfig.CLUSTER_NAME))
                .name("RuleEvaluatorFunction")
                .uid("rule-evaluator-function");

        ClickHouseSinkConfig chConfig = ClickHouseSinkConfig.builder()
                .hosts(Collections.singletonList(AuthDemoConfig.CH_HOSTS))
                .user(AuthDemoConfig.CH_USER)
                .password(AuthDemoConfig.CH_PASSWORD)
                .database(AuthDemoConfig.CH_DATABASE)
                .table(AuthDemoConfig.CH_TABLE)
                .autoCreateDdl(false)
                .useDistributed(true)
                .maxBufferSize(AuthDemoConfig.CH_BATCH_SIZE)
                .flushIntervalMs(AuthDemoConfig.CH_FLUSH_INTERVAL_MS)
                .build();

        results.sinkTo(ClickHouseSinkBuilder.build(chConfig))
                .name("ClickHouseSink")
                .uid("clickhouse-sink")
                .setParallelism(3);

        KafkaSink<AggregationResult> kafkaResultsSink = KafkaSink.<AggregationResult>builder()
                .setBootstrapServers(AuthDemoConfig.KAFKA_BOOTSTRAP)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(AuthDemoConfig.RESULTS_TOPIC)
                        .setKeySerializationSchema((SerializationSchema<AggregationResult>) r ->
                                r.getRuleId() != null ? r.getRuleId().getBytes() : new byte[0])
                        .setValueSerializationSchema(new ResultSerializationSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        results.sinkTo(kafkaResultsSink)
                .name("KafkaResultsSink")
                .uid("kafka-results-sink")
                .setParallelism(3);

        log.info("Executing Velocity Engine Auth Demo");
        env.execute("UIDAI Velocity Engine Auth Demo");
    }
}