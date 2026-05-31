# UIDAI Velocity Engine - Flink Job (Auth Demo)

## Overview
This repository contains the Apache Flink engine for the UIDAI Velocity Engine Control Plane. This job is a highly optimized, dynamically programmable rule evaluation engine designed to process massive volumes of authentication telemetry in real-time.

It achieves true zero-downtime programmability via **Broadcast State**. Analysts can define aggregations, filters, window lengths, and threshold logic from the Control Plane UI. The JSON payload is published to Kafka, broadcasted instantly to all Flink task managers, and applied to the incoming data stream on the fly.

## Code Execution Flow
1. **Event Ingestion**: `AuthDemoPipeline.java` creates a `KafkaSource` tracking the `auth-topic`.
2. **Rule Ingestion**: A secondary `KafkaSource` tracks the `velocity-rules` topic and uses `kafkaRules.broadcast(...)` to mirror the analyst configurations across the cluster.
3. **Dynamic Routing**: `DynamicKeyFunction.java` intercepts raw events, evaluates them against active rule filters, extracts the desired group key (e.g. `_data.uid`), and clones the event for downstream routing.
4. **State Bucketing**: `RuleEvaluatorFunction.java` intercepts the keyed stream. It bypasses native Flink windows, passing events to `BucketStateManager.java`.
5. **Aggregation & Timers**: `BucketStateManager` securely computes complex aggregations (SUM, MIN, MAX, exact distinct counts, and HyperLogLog cardinality). Once the window closes (driven by highly optimized processing-time timers), the state is pruned.
6. **Alert Evaluation**: The results are compared against user-defined Thresholds (`HavingEvaluator.java`). 
7. **Sinks**: All results are flushed to ClickHouse (via `ClickHouseSinkConfig`) for live frontend telemetry, and breaches trigger distinct alerts.

## Production Optimizations Included
- **OOM Prevention**: Aggregation state map is strictly evaluated using `isEmpty()` logic. Dormant user keys are pruned instantly, stopping infinite timers.
- **Checkpointing**: Aligned EXACTLY_ONCE checkpointing with `RETAIN_ON_CANCELLATION` enabled.
- **Watermarking**: Global watermark bound to Kafka Record Timestamp to guarantee monotonic progression, with fallback parsing for custom timestamps inside the evaluator.

## Build Instructions
```bash
mvn clean package -DskipTests
```
This generates the fat-jar target: `target/uid-dp-velocity-engine-job-1.0.jar`
