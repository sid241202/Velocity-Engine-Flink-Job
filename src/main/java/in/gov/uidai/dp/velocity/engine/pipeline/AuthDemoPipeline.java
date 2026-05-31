package in.gov.uidai.dp.velocity.engine.pipeline;

import in.gov.uidai.dp.velocity.engine.config.AuthDemoConfig;
import in.gov.uidai.dp.velocity.engine.deserializers.EventDeserializer;
import in.gov.uidai.dp.velocity.engine.deserializers.RuleDeserializer;
import in.gov.uidai.dp.velocity.engine.functions.AuthDeduplicationFunction;
import in.gov.uidai.dp.velocity.engine.functions.DynamicKeyFunction;
import in.gov.uidai.dp.velocity.engine.functions.RuleEvaluatorFunction;
import in.gov.uidai.dp.velocity.engine.model.AggregationResult;
import in.gov.uidai.dp.velocity.engine.model.Event;
import in.gov.uidai.dp.velocity.engine.model.Keyed;
import in.gov.uidai.dp.velocity.engine.model.VelocityRule;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;

import in.gov.uidai.dp.velocity.engine.config.ClickHouseSinkConfig;
import in.gov.uidai.dp.velocity.engine.sinks.ClickHouseSinkBuilder;

import java.time.Duration;
import java.util.Collections;

@Slf4j
public class AuthDemoPipeline {

        public void buildAndExecute() throws Exception {
                Configuration conf = new Configuration();
                conf.setString("state.backend.rocksdb.options-factory",
                                "in.gov.uidai.dp.velocity.engine.pipeline.RocksDBOptions");
                conf.setString("state.checkpoints.dir", AuthDemoConfig.CHECKPOINT_STORAGE);
                StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

                env.setParallelism(16);

                env.enableCheckpointing(AuthDemoConfig.CHECKPOINT_INTERVAL_MS, CheckpointingMode.EXACTLY_ONCE);
                env.getCheckpointConfig().setCheckpointTimeout(AuthDemoConfig.CHECKPOINT_TIMEOUT_MS);
                env.getCheckpointConfig().setMinPauseBetweenCheckpoints(AuthDemoConfig.CHECKPOINT_MIN_PAUSE_MS);
                env.getCheckpointConfig().setExternalizedCheckpointRetention(
                                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
                env.getCheckpointConfig().enableUnalignedCheckpoints();

                KafkaSource<Event> kafkaSource = KafkaSource.<Event>builder()
                                .setBootstrapServers(AuthDemoConfig.KAFKA_BOOTSTRAP_SERVERS)
                                .setTopics(AuthDemoConfig.AUTH_TOPIC)
                                .setGroupId(AuthDemoConfig.CONSUMER_GROUP)
                                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                                .setDeserializer(new EventDeserializer(AuthDemoConfig.AUTH_TOPIC, "auth-cluster",
                                                "_event_timestamp", "ISO_STRING"))
                                .build();

                WatermarkStrategy<Event> watermarkStrategy = WatermarkStrategy
                                .<Event>forBoundedOutOfOrderness(Duration.ZERO)
                                .withIdleness(Duration.ofMillis(AuthDemoConfig.IDLENESS_MS));

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
                                .setBootstrapServers(AuthDemoConfig.KAFKA_BOOTSTRAP_SERVERS)
                                .setTopics(AuthDemoConfig.RULES_TOPIC)
                                .setGroupId(AuthDemoConfig.RULES_CONSUMER_GROUP)
                                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                                .setDeserializer(
                                                new in.gov.uidai.dp.velocity.engine.deserializers.RuleDeserializer())
                                .build();

                DataStream<VelocityRule> kafkaRules = env
                                .fromSource(rulesSource, WatermarkStrategy.noWatermarks(), "Kafka-Rules")
                                .uid("kafka-rules");

                BroadcastStream<VelocityRule> broadcastRules = kafkaRules.broadcast(DynamicKeyFunction.RULE_STATE_DESC);

                String clusterName = "auth-cluster";

                SingleOutputStreamOperator<Keyed<Event, String, String>> keyedEvents = deduplicatedEvents
                                .connect(broadcastRules)
                                .process(new DynamicKeyFunction(clusterName))
                                .name("DynamicKeyFunction")
                                .uid("dynamic-key-function");

                SingleOutputStreamOperator<AggregationResult> results = keyedEvents

                                .keyBy(keyed -> keyed.getId() + "|" + keyed.getKey())
                                .connect(broadcastRules)
                                .process(new RuleEvaluatorFunction(clusterName))
                                .name("RuleEvaluatorFunction")
                                .uid("rule-evaluator-function");

                ClickHouseSinkConfig chConfig = new ClickHouseSinkConfig();
                chConfig.setHosts(Collections.singletonList(AuthDemoConfig.CLICKHOUSE_HOSTS));
                chConfig.setUser(AuthDemoConfig.CLICKHOUSE_USER);
                chConfig.setPassword(AuthDemoConfig.CLICKHOUSE_PASSWORD);
                chConfig.setDatabase(AuthDemoConfig.CLICKHOUSE_DATABASE);
                chConfig.setTable(AuthDemoConfig.CLICKHOUSE_TABLE);
                chConfig.setAutoCreateDdl(false);// TODO
                chConfig.setMaxBufferSize(5000);

                results.sinkTo(ClickHouseSinkBuilder.build(chConfig))
                                .name("ClickHouseSink")
                                .uid("clickhouse-sink")
                                .setParallelism(4);

                results.getSideOutput(RuleEvaluatorFunction.ALERT_TAG)
                                .print()
                                .name("AlertLogger")
                                .uid("alert-logger");

                log.info("Executing Velocity Engine Auth Demo");
                env.execute("UIDAI Velocity Engine Auth Demo");
        }
}