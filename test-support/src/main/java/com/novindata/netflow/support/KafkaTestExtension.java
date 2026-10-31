package com.novindata.netflow.support;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.kafka.KafkaContainer;

public final class KafkaTestExtension implements BeforeAllCallback, AfterAllCallback {

    private static final String KAFKA_IMAGE = "apache/kafka:4.2.1";

    private final KafkaContainer kafka = new KafkaContainer(KAFKA_IMAGE);

    @Override
    public void beforeAll(ExtensionContext context) {
        kafka.start();
    }

    @Override
    public void afterAll(ExtensionContext context) {
        kafka.stop();
    }

    public String getBootstrapServers() {
        if (!kafka.isRunning()) {
            throw new IllegalStateException("Kafka test container is not running");
        }
        return kafka.getBootstrapServers();
    }
}
