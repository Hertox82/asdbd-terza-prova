FROM eclipse-temurin:11.0.26_4-jre-jammy AS java
FROM python:3.11.11-slim-bookworm

COPY --from=java /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk \
    PYTHONPATH=/workspace \
    PYSPARK_PYTHON=python \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH=/opt/java/openjdk/bin:$PATH

ARG JDBC_VERSION=42.7.5
RUN pip install --no-cache-dir pyspark==3.5.5 jupyterlab==4.3.6 ipykernel==6.29.5 \
        matplotlib==3.10.1 'psycopg[binary]==3.2.6' \
    && python -c "from urllib.request import urlretrieve; urlretrieve('https://repo.maven.apache.org/maven2/org/postgresql/postgresql/${JDBC_VERSION}/postgresql-${JDBC_VERSION}.jar', '/opt/postgresql.jar')"

RUN apt-get update \
    && apt-get install -y --no-install-recommends procps \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 1000 tlc
USER tlc
WORKDIR /workspace
EXPOSE 8888
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--ServerApp.root_dir=/workspace", "--ServerApp.show_banner=False"]
