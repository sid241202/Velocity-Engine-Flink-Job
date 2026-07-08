# Flink Job — uid-dp-velocity-engine-auth-flink-job

Flink 2.2.0 stateful stream processing job that evaluates velocity/anomaly
rules over the raw auth-event stream. See `../CLAUDE.md` (also auto-loaded)
for cross-repo rules, the session-continuity protocol, and the
autonomy/permission scope — this file only covers what's specific to this repo.

## Stack

- Java 17, Maven (`pom.xml`), Flink `2.2.0`, `flink-kafka-connector` `4.0.1-2.0`.
- Hand-rolled stateful processing (`KeyedBroadcastProcessFunction` /
  `BroadcastProcessFunction`, `RocksDBOptions`, `MapState`, State TTL,
  broadcast rule state, early-fire/partial emission) — **not** Flink's
  `WindowedStream`/Trigger DSL.
- Package root: `in.gov.uidai.dp.velocity.engine`.

## Key classes

- `pipeline/AuthDemoPipeline.java` — the job graph.
- `functions/RuleEvaluatorFunction.java` — core rule evaluation.
- `functions/AuthDeduplicationFunction.java`, `functions/DynamicKeyFunction.java`.
- `deserializers/EventDeserializer.java`, `RuleDeserializer.java`.
- `aggregation/BucketStateManager.java` + `aggregation/functions/*Accumulator.java`
  (`Count`, `Sum`, `Avg`, `Min`, `Max`, `CountDistinctExact`, `CountDistinctHll`).
  `CountDistinctExact` was fixed to an O(1)-add storage representation
  (was previously re-serializing/scanning on every add).
- `sinks/ClickHouseSinkBuilder.java`, `ClickHouseResultConverter.java`,
  `ClickHouseDdlInitializer.java`, `sinks/RedisSink.java` — Redis penalty
  keys are per-entity (fixed from a prior shared/global-key design).
- `model/VelocityRule.java` and friends — the rule schema shared (as JSON)
  with the backend via `DE.AUTH.VELOCITY_ENGINE.RULES`.
- `config/AuthDemoConfig.java`, `ClickHouseSinkConfig.java`, `RedisConfig.java`.

## Kafka topics (see `../CLAUDE.md` for the full data-flow picture)

Consumes `BI.AUTH.AUTH_TXN.UNION.V1` (raw auth events) +
`DE.AUTH.VELOCITY_ENGINE.RULES` (broadcast rule definitions). Produces
`DE.AUTH.VELOCITY_ENGINE.RESULTS` and `DE.AUTH.VELOCITY_ENGINE.ANOMALIES` —
schema changes here need the backend consumer and ClickHouse DDL updated in
the same change (cross-repo contract).

## Remote/branches

This repo has **only a `github` remote** (no `origin`/Bitbucket, unlike the
other two repos) — verify with `git remote -v` before assuming both exist.
`release` is the branch in use; no dedicated feature branch has been needed
here yet for the RBAC work (RBAC is a control-plane concern, not a streaming
concern).

## Build

`mvn -o compile` (offline mode has worked in this environment; drop `-o` if
that ever fails to pick up a new dependency). No local run/test harness has
been exercised in this engagement beyond compilation — verify current state
with a fresh `mvn compile` rather than assuming test coverage exists.
