@echo off
curl -X POST "http://localhost:9090/v1/jars/82497fc0-7ed7-41f7-aea3-f914d75940b5_uid-dp-velocity-engine-auth-flink-job-1.0.0-RELEASE.jar/run" ^
  -H "Content-Type: application/json" ^
  -d "{\"entryClass\":\"in.gov.uidai.dp.velocity.engine.AuthDemoDriver\",\"parallelism\":1}"
