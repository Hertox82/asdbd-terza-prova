FROM eclipse-temurin:11.0.26_4-jre-jammy

ARG HADOOP_VERSION=3.3.6
ENV HADOOP_HOME=/opt/hadoop \
    HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop \
    HADOOP_LOG_DIR=/tmp/hadoop-logs \
    HADOOP_HEAPSIZE_MAX=512m \
    PATH=/opt/hadoop/bin:$PATH

RUN wget -q "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" -O /tmp/hadoop.tar.gz \
    && mkdir -p /opt/hadoop /var/lib/hdfs \
    && tar -xzf /tmp/hadoop.tar.gz --strip-components=1 -C /opt/hadoop \
    && rm /tmp/hadoop.tar.gz \
    && useradd --create-home hdfs \
    && chown hdfs:hdfs /var/lib/hdfs

COPY docker/hdfs/core-site.xml docker/hdfs/hdfs-site.xml /opt/hadoop/etc/hadoop/
COPY docker/hdfs/start.sh /opt/start-hdfs.sh
USER hdfs
CMD ["bash", "/opt/start-hdfs.sh"]
