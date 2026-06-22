# Spark job image: pinned base + pinned GCS connector + baked-in PySpark jobs.
# Versions mirror infra/versions.env (Design §15: pin, never -latest).
ARG SPARK_VERSION=3.5.6
FROM apache/spark:${SPARK_VERSION}

USER root

# GCS connector (hadoop3, shaded). The hadoop-lib bucket serves the shaded jar
# without a -shaded suffix. ADD from URL lands as mode 600, so make it readable.
ARG GCS_CONNECTOR_VERSION=2.2.33
ADD https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-${GCS_CONNECTOR_VERSION}.jar \
    /opt/spark/jars/gcs-connector-hadoop3.jar
RUN chmod 0644 /opt/spark/jars/gcs-connector-hadoop3.jar

# PySpark jobs (aggregate.py imports sibling schema.py; spark-submit adds this dir to sys.path).
COPY jobs/ /opt/spark/jobs/

USER spark
