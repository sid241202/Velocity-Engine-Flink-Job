@echo off
cd /d E:\Downloads\uid-dp-velocity-engine-auth-flink-job
git add src/main/java/in/gov/uidai/dp/velocity/engine/pipeline/AuthDemoPipeline.java
git commit -m "fix: rules source uses committedOffsets with LATEST fallback to avoid re-consuming stale rules"
