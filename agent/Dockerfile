FROM python:3.12-slim
WORKDIR /srv
# optional drivers: Postgres, MySQL/MariaDB, MongoDB, Kafka (Redis, ClickHouse, Elasticsearch, RabbitMQ need none)
# optional cloud SDKs: Snowflake, AWS SQS/CloudWatch (boto3), Google Pub/Sub, Azure Monitor
ARG WITH_SNOWFLAKE=false
ARG WITH_AWS=false
ARG WITH_GCP=false
ARG WITH_AZURE=false
RUN pip install --no-cache-dir "psycopg[binary]>=3.1" "PyMySQL>=1.1" "pymongo>=4.8" "PyYAML>=6" "confluent-kafka>=2.5" \
 && if [ "$WITH_SNOWFLAKE" = "true" ]; then pip install --no-cache-dir "snowflake-connector-python>=3.12"; fi \
 && if [ "$WITH_AWS" = "true" ]; then pip install --no-cache-dir "boto3>=1.35"; fi \
 && if [ "$WITH_GCP" = "true" ]; then pip install --no-cache-dir "google-cloud-monitoring>=2.22"; fi \
 && if [ "$WITH_AZURE" = "true" ]; then pip install --no-cache-dir "azure-monitor-querymetrics>=1.0" "azure-monitor-query>=2.0" "azure-identity>=1.19"; fi
# non-root, matching the Kubernetes securityContext in deploy/kubernetes/agent.yaml
RUN useradd --uid 10001 --no-create-home mdjaada && mkdir /data && chown 10001 /data
USER 10001
COPY mdjaada_agent mdjaada_agent
ENV MDJAADA_DATA_DIR=/data
VOLUME /data
EXPOSE 4318
CMD ["python", "-m", "mdjaada_agent"]
