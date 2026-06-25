package in.gov.uidai.dp.velocity.engine.functions;

import in.gov.uidai.dp.velocity.engine.model.Event;
import in.gov.uidai.dp.velocity.engine.model.Keyed;
import in.gov.uidai.dp.velocity.engine.model.VelocityRule;
import in.gov.uidai.dp.velocity.engine.utils.FilterEvaluator;
import in.gov.uidai.dp.velocity.engine.utils.KeysExtractor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Map;

@Slf4j
public class DynamicKeyFunction extends BroadcastProcessFunction<Event, VelocityRule, Keyed<Event, String, String>> {

    private static final long serialVersionUID = 1L;

    public static final MapStateDescriptor<String, VelocityRule> RULE_STATE_DESC =
            new MapStateDescriptor<>(
                    "rules-broadcast-state",
                    Types.STRING,
                    TypeInformation.of(new TypeHint<VelocityRule>() {})
            );

    public DynamicKeyFunction() {}

    @Override
    public void processElement(Event event, ReadOnlyContext ctx, Collector<Keyed<Event, String, String>> out) throws Exception {
        ReadOnlyBroadcastState<String, VelocityRule> rulesState = ctx.getBroadcastState(RULE_STATE_DESC);

        String eventSourceTopic = String.valueOf(event.getFields().get("_source_topic"));
        if (eventSourceTopic == null || "null".equals(eventSourceTopic)) {
            return;
        }

        for (Map.Entry<String, VelocityRule> entry : rulesState.immutableEntries()) {
            VelocityRule rule = entry.getValue();

            if (!rule.isActive()) continue;
//            if (!cluster.equalsIgnoreCase(rule.getSourceCluster())) continue;
//            if (!eventSourceTopic.equalsIgnoreCase(rule.getSourceTopic())) continue; TODO

            if (FilterEvaluator.evaluate(event, rule.getFilters())) {
                String groupKey = KeysExtractor.getKey(rule.getGrouping().getKeys(), event);
                out.collect(new Keyed<>(event, groupKey, rule.getRuleId()));
            }
        }
    }

    @Override
    public void processBroadcastElement(VelocityRule rule, Context ctx, Collector<Keyed<Event, String, String>> out) throws Exception {
        BroadcastState<String, VelocityRule> rulesState = ctx.getBroadcastState(RULE_STATE_DESC);

        if (rule.isDeleted()) {
            rulesState.remove(rule.getRuleId());
            log.info("Rule DELETED from broadcast state: {}", rule.getRuleId());
        } else {
            rulesState.put(rule.getRuleId(), rule);
            log.info("Rule updated in broadcast state: {} (status: {})", rule.getRuleId(), rule.getStatus());
        }
    }
}