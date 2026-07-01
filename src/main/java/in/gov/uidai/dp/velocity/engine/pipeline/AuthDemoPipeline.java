package in.gov.uidai.dp.velocity.engine.pipeline;

import in.gov.uidai.dp.velocity.engine.config.AuthDemoConfig;
import in.gov.uidai.dp.velocity.engine.config.RedisConfig;
import in.gov.uidai.dp.velocity.engine.deserializers.EventDeserializer;
import in.gov.uidai.dp.velocity.engine.deserializers.RuleDeserializer;
import in.gov.uidai.dp.velocity.engine.functions.AuthDeduplicationFunction;
import in.gov.uidai.dp.velocity.engine.functions.DynamicKeyFunction;
import in.gov.uidai.dp.velocity.engine.functions.RuleEvaluatorFunction;
import in.gov.uidai.dp.velocity.engine.model.AggregationResult;
import in.gov.uidai.dp.velocity.engine.model.AnomalyEvent;
import in.gov.uidai.dp.velocity.engine.model.Event;
import in.gov.uidai.dp.velocity.engine.model.Keyed;
import in.gov.uidai.dp.velocity.engine.model.VelocityRule;
import in.gov.uidai.dp.velocity.engine.sinks.RedisSink;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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

@Slf4j
public class AuthDemoPipeline {

        public void buildAndExecute() throws Exception {
                Configuration conf = new Configuration();
                conf.setString("state.backend.rocksdb.options-factory",
                                "in.gov.uidai.dp.velocity.engine.pipeline.RocksDBOptions");
                conf.setString("state.checkpoints.dir", AuthDemoConfig.CHECKPOINT_DIR);
                StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
                // NOTE: Do NOT set global parallelism=1 here — that forces 80M+/day events
                // through a single task thread, which is a fatal throughput bottleneck.
                // Parallelism is configured in flink-conf.yaml / k8s deployment.
                // The rules source is deliberately kept at parallelism=1 (broadcast constraint).
                env.enableCheckpointing(60_000L, CheckpointingMode.EXACTLY_ONCE);
                env.getCheckpointConfig().setCheckpointTimeout(600_000L);
                env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10_000L);
                // Tolerate up to 3 consecutive checkpoint failures before failing the job.
                // Without this, a single transient checkpoint failure brings down the pipeline.
                env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
                env.getCheckpointConfig().setExternalizedCheckpointRetention(
                                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
                env.getCheckpointConfig().enableUnalignedCheckpoints();

                KafkaSource<Event> kafkaSource = KafkaSource.<Event>builder()
                                .setBootstrapServers(AuthDemoConfig.KAFKA_BOOTSTRAP)
                                .setTopics(AuthDemoConfig.AUTH_TOPIC)
                                .setGroupId(AuthDemoConfig.AUTH_CONSUMER_GROUP)
                                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                                .setDeserializer(new EventDeserializer(AuthDemoConfig.AUTH_TOPIC,
                                                AuthDemoConfig.EVENT_TIMESTAMP_FIELD,
                                                AuthDemoConfig.EVENT_TIMESTAMP_FORMAT))
                                .build();

                SingleOutputStreamOperator<Event> deduplicatedEvents = env
                                .fromSource(kafkaSource, WatermarkStrategy
                                                .<Event>forBoundedOutOfOrderness(
                                                                Duration.ofMillis(AuthDemoConfig.MAX_WATERMARK_LAG_MS))
                                                .withIdleness(Duration.ofSeconds(30)), "Kafka-Auth-Events")
                                .uid("kafka-auth-events")
                                .keyBy(event -> {
                                        Object a = in.gov.uidai.dp.velocity.engine.utils.FieldExtractor
                                                        .extractObject(event, "_data.authCode");
                                        return a != null ? a.toString() : "";
                                })
                                .process(new AuthDeduplicationFunction()).name("AuthDeduplicator")
                                .uid("auth-deduplicator");

                BroadcastStream<VelocityRule> broadcastRules = env
                                .fromSource(KafkaSource.<VelocityRule>builder()
                                                .setBootstrapServers(AuthDemoConfig.KAFKA_BOOTSTRAP)
                                                .setTopics(AuthDemoConfig.RULES_TOPIC)
                                                .setGroupId(AuthDemoConfig.RULES_CONSUMER_GROUP)
                                                .setStartingOffsets(OffsetsInitializer
                                                                .committedOffsets(OffsetResetStrategy.LATEST))
                                                .setDeserializer(new RuleDeserializer()).build(),
                                                WatermarkStrategy.<VelocityRule>forMonotonousTimestamps()
                                                                .withIdleness(Duration.ofSeconds(30)),
                                                "Kafka-Rules")
                                .uid("kafka-rules").setParallelism(1)
                                .broadcast(DynamicKeyFunction.RULE_STATE_DESC);

                SingleOutputStreamOperator<AggregationResult> results = deduplicatedEvents
                                .connect(broadcastRules)
                                .process(new DynamicKeyFunction()).name("DynamicKeyFunction")
                                .uid("dynamic-key-function")
                                .keyBy(keyed -> keyed.getId() + "|" + keyed.getKey())
                                .connect(broadcastRules)
                                .process(new RuleEvaluatorFunction())
                                .name("RuleEvaluatorFunction").uid("rule-evaluator-function");

                // Agg Kafka Sink
                results.sinkTo(KafkaSink.<AggregationResult>builder()
                                .setBootstrapServers(AuthDemoConfig.KAFKA_BOOTSTRAP)
                                .setRecordSerializer(KafkaRecordSerializationSchema.<AggregationResult>builder()
                                                .setTopic(AuthDemoConfig.RESULTS_TOPIC)
                                                .setKeySerializationSchema(r -> r.getId() != null ? r.getId().getBytes()
                                                                : new byte[0])
                                                .setValueSerializationSchema(new ResultSerializationSchema()).build())
                                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE).build())
                                .name("AggKafkaSink").uid("agg-kafka-sink");

                // Anomaly Kafka Sink
                DataStream<AnomalyEvent> anomalyStream = results.getSideOutput(RuleEvaluatorFunction.ANOMALY_TAG);
                anomalyStream.sinkTo(KafkaSink.<AnomalyEvent>builder()
                                .setBootstrapServers(AuthDemoConfig.KAFKA_BOOTSTRAP)
                                .setRecordSerializer(KafkaRecordSerializationSchema.<AnomalyEvent>builder()
                                                .setTopic(AuthDemoConfig.ANOMALY_TOPIC)
                                                .setKeySerializationSchema(e -> e.getId() != null ? e.getId().getBytes()
                                                                : new byte[0])
                                                .setValueSerializationSchema(new AnomalySerializationSchema()).build())
                                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE).build())
                                .name("AnomalyKafkaSink").uid("anomaly-kafka-sink");

                // Redis Anomaly Store Sink
                results.getSideOutput(RuleEvaluatorFunction.REDIS_TAG)
                                .sinkTo(new RedisSink(RedisConfig.fromConfig(), AuthDemoConfig.REDIS_DEFAULT_TTL_SECONDS))
                                .name("RedisAnomalyStoreSink").uid("redis-anomaly-store-sink");

                log.info("Executing UIDAI Velocity Engine — 3-sink: AggKafka + AnomalyKafka + Redis");
                env.execute("UIDAI Velocity Engine Auth Demo");
        }
}