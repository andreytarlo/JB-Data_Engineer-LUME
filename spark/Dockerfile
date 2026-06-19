FROM bitnamilegacy/spark:3.5.1

USER root

# Install curl (for JAR downloads) and extra Python packages.
RUN install_packages curl && \
    pip install --no-cache-dir "psycopg[binary]>=3.1" "redis>=5.0"

# Pre-bake the Spark ↔ Kafka connector JARs so the container works
# without internet access at runtime.
ARG JARS=/opt/bitnami/spark/jars
ARG SPARK_VER=3.5.0
ARG SCALA_VER=2.12

RUN curl -fsSL \
      "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_${SCALA_VER}/${SPARK_VER}/spark-sql-kafka-0-10_${SCALA_VER}-${SPARK_VER}.jar" \
      -o "${JARS}/spark-sql-kafka.jar" && \
    curl -fsSL \
      "https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_VER}/${SPARK_VER}/spark-token-provider-kafka-0-10_${SCALA_VER}-${SPARK_VER}.jar" \
      -o "${JARS}/spark-token-provider-kafka.jar" && \
    curl -fsSL \
      "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar" \
      -o "${JARS}/kafka-clients.jar" && \
    curl -fsSL \
      "https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar" \
      -o "${JARS}/commons-pool2.jar"

COPY jobs/ /app/jobs/

# Create checkpoint dir owned by the bitnami runtime user (1001).
# Docker copies this into the named volume on first mount, so the volume
# starts with the correct ownership even though it is initially empty.
RUN mkdir -p /checkpoint && chown 1001:1001 /checkpoint

USER 1001
WORKDIR /app/jobs
