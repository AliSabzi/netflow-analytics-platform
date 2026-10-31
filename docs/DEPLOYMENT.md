# Deployment guide

These templates require your own infrastructure and configuration. They do not include a running cluster, data generator or traffic dataset.

## Build application images

Run from the repository root with JDK 17, Maven and Docker available:

```bash
mvn -B -ntp -DskipTests install
mvn -B -ntp -pl hdfs-writer compile jib:dockerBuild -Dimage=ghcr.io/alisabzi/hdfs-writer:portfolio
mvn -B -ntp -pl starrocks-writer compile jib:dockerBuild -Dimage=ghcr.io/alisabzi/starrocks-writer:portfolio
mvn -B -ntp -pl rest-api-module compile jib:dockerBuild -Dimage=ghcr.io/alisabzi/rest-api:portfolio
mvn -B -ntp -pl spark-analyzer compile jib:dockerBuild -Dimage=ghcr.io/alisabzi/spark-analyzer:portfolio
mvn -B -ntp -pl behavioral-clustering compile jib:dockerBuild -Dimage=ghcr.io/alisabzi/behavioral-clustering:portfolio
```

These commands build local images; they do not push images to a registry. For Kubernetes, push your own built images to a registry accessible to the cluster and set `global.registry` and `global.imageTag` accordingly.

The root Dockerfile is an alternative build for the HDFS writer only:

```bash
docker build -t ghcr.io/alisabzi/hdfs-writer:portfolio .
```

## Configure application containers

```bash
cp .env.example .env
mkdir -p .local/hadoop
cp hdfs-writer/src/main/resources/core-site.xml .local/hadoop/core-site.xml
cp hdfs-writer/src/main/resources/hdfs-site.xml .local/hadoop/hdfs-site.xml
```

Edit `.env` and the copied Hadoop XML files. Replace the example nameservice, node names and addresses with your own. The original XML files illustrate an HDFS HA configuration; for a single NameNode use the appropriate Hadoop configuration for that environment.

Provide Kafka and schema registry endpoints, database URLs and credentials. The writer account needs the privileges used by the StarRocks schema initializer and Stream Load. Configure the API accounts for the queries they will execute.

Initialize the required database tables from the SQL under `spark-analyzer/src/main/resources/db` and `starrocks-writer/src/main/resources`. The Spark SQL files include alternative schemas and example queries; choose the schema matching the jobs and repositories you run.

```bash
# Validate configuration without starting containers
docker compose config --quiet

# Start long-running application services
docker compose up -d hdfs-writer starrocks-writer rest-api

# Run one Spark aggregation batch
docker compose --profile batch run --rm spark-analyzer
```

Compose mounts the local Hadoop files over the image's example resources. External Kafka, HDFS and StarRocks hostnames must resolve from the containers. Spark workers must also be able to reach `SPARK_DRIVER_HOST`; configure driver networking and firewall rules for your environment. Kubernetes CronJobs provide the scheduled batch deployment alternative.

The two ingestion paths use `netflow` by default in Compose. Keep the topic and Avro serialization contract consistent across producers, schemas and both writers.

## Kubernetes and Helm

The umbrella chart uses externally configured Kafka, HDFS, Spark and database infrastructure. It includes a schema registry subchart. KEDA is required for the `ScaledObject` resources. Configure storage classes, service exposure, the `hadoop-config` ConfigMap, node aliases and service DNS for your cluster.

Example values under `192.0.2.*` and `netflow-demo` are topology examples. Replace them consistently, including the Hadoop XML files and monitoring targets. Review Kafka topics in the HDFS writer values so they match the other ingestion path.

Supply credentials in an ignored local values file, then render before deployment:

```bash
helm lint helm-chart -f .local/values.yaml
helm template netflow-release helm-chart -f .local/values.yaml > .local/rendered.yaml
```

The local values must set at least `secrets.postgresPassword` and `secrets.starrocksPassword`, plus endpoints and image settings for your cluster. Review the generated resources, then deploy them using your usual Kubernetes release process.

Infrastructure charts are under `infrastructure/charts`. Monitoring requires a Grafana admin password; the PostgreSQL chart requires superuser, replication and application passwords. The custom PostgreSQL/Patroni image has its own Dockerfile in the chart directory and must be built and pushed separately.

## Ansible examples

The Kafka and StarRocks inventories are named `inventory/hosts.example.ini`. Copy the chosen file to `inventory/hosts.ini`, which is ignored by Git, and edit the addresses and SSH user.

Artifact download URLs use `artifacts.example.com`. Replace them with real URLs for the configured versions and verify their checksums. Generate fresh Kafka cluster and controller directory IDs for a new cluster. Example IDs must not be reused for an existing cluster.

The optional `ci/jenkins/Jenkinsfile.backfill` illustrates a manual StarRocks backfill job. It needs a configured Jenkins agent with Helm, kubectl and access to the target cluster.

## Schemas and credentials

Avro record names in this version use `com.novindata.netflow.core.avro`. Treat a change of fully qualified record name as a schema compatibility concern when reusing an existing registry or serialized records.

Runtime passwords are supplied through environment variables or Kubernetes Secrets. Empty Helm password values are intentional. Test-only credentials in test fixtures and CI lint examples are synthetic and are not deployment credentials.
