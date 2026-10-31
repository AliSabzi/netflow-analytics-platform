FROM maven:3.9-eclipse-temurin-17 AS builder
WORKDIR /build
COPY . .
RUN mvn -B -ntp -pl hdfs-writer -am -DskipTests install \
    && mvn -B -ntp -pl hdfs-writer dependency:copy-dependencies \
        -DincludeScope=runtime -DoutputDirectory=target/dependency

FROM eclipse-temurin:17-jre
WORKDIR /app
RUN groupadd --system netflow && useradd --system --gid netflow netflow
COPY --from=builder /build/hdfs-writer/target/hdfs-writer-1.0-SNAPSHOT.jar /app/app.jar
COPY --from=builder /build/hdfs-writer/target/dependency/ /app/libs/
COPY --from=builder /build/hdfs-writer/src/main/resources/ /app/resources/
USER netflow
ENV HADOOP_USER_NAME=hadoop
EXPOSE 9400
ENTRYPOINT ["java", "-cp", "/app/resources:/app/app.jar:/app/libs/*", "com.novindata.netflow.Application"]
