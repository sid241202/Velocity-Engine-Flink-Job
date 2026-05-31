package in.gov.uidai.dp.velocity.engine.functions;

import in.gov.uidai.dp.velocity.engine.model.Event;
import in.gov.uidai.dp.velocity.engine.utils.FieldExtractor;

import java.time.Duration;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class AuthDeduplicationFunction extends KeyedProcessFunction<String, Event, Event> {

    private transient ValueState<Boolean> seenState;

    @Override
public void open(OpenContext parameters) {

        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Duration.ofMinutes(15))
                .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .cleanupInRocksdbCompactFilter(1000)
                .build();

        ValueStateDescriptor<Boolean> desc = new ValueStateDescriptor<>("deduplicator", Boolean.class);
        desc.enableTimeToLive(ttlConfig);
        seenState = getRuntimeContext().getState(desc);
    }

    @Override
public void processElement(Event event, Context ctx, Collector<Event> out) throws Exception {

        String authCode = FieldExtractor.extractString(event, "_data.authCode");

        if (authCode == null || authCode.isEmpty()) {

            return;
        }

        if (seenState.value() == null) {
            seenState.update(true);
            out.collect(event);
        }
    }
}