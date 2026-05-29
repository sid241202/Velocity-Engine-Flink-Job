FROM mndc-harbor-registry-non-prod.uidai.net.in/data-platform/flinkv2.2.0:2.0.1-RELEASE

COPY target/*.jar /opt/flink/usrlib/
