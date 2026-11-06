# NetFlow Analytics Platform

A team-built data platform for ingesting, processing, storing, and querying network flow records. The project was developed during a nine-week big data engineering internship.

## How the platform is organized

- **Ingestion and storage** — Kafka carries flow events. The [HDFS writer](hdfs-writer/) persists data to HDFS, and the [StarRocks writer](starrocks-writer/) loads data for analytical queries.
- **Processing and access** — [Spark](spark-analyzer/) processes flow data. The platform also includes [behavioral clustering](behavioral-clustering/) and a [REST API](rest-api-module/).
- **Deployment** — The [Helm chart](helm-chart/) and [infrastructure files](infrastructure/) contain the Kubernetes deployment configuration. [Docker Compose](docker-compose.yml) and the [environment template](.env.example) are also available in the repository.

## My contributions

**Ali Sabzi** worked on the following components of the team project:

- **PostgreSQL on Kubernetes** — Deployed a highly available PostgreSQL cluster using Helm and Patroni.
- **Observability** — Set up a Helm-managed Kubernetes monitoring stack with Prometheus exporters and alerting, Grafana dashboards, and application and infrastructure metrics.
- **Autoscaling** — Implemented KEDA-based scaling for the HDFS and StarRocks writers.
- **StarRocks writer** — Developed the Java writer with Kafka partition batching, schema initialization, dead-letter handling, HDFS backfill, metrics, and automated tests.

## Where to start

For the infrastructure and monitoring work, start with the [Helm chart](helm-chart/) and [infrastructure configuration](infrastructure/). For the ingestion service, start with the [StarRocks writer](starrocks-writer/).
