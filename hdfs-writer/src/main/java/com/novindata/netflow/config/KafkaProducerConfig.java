package com.novindata.netflow.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig {

    @Value("${app.kafka.dlq-topic}")
    private String dlqTopic;

    @Value("${app.kafka.dlq-partitions:3}")
    private int dlqPartitions;

    @Value("${app.kafka.dlq-replication-factor:3}")
    private short dlqReplicationFactor;

    @Value("${app.kafka.late-topic}")
    private String lateTopic;

    @Value("${app.kafka.late-topic-partitions:24}")
    private int lateTopicPartitions;

    @Value("${app.kafka.late-topic-replication-factor:3}")
    private short lateTopicReplicationFactor;

    @Value("${app.kafka.watermark-topic}")
    private String watermarkTopic;

    @Value("${app.kafka.watermark-topic-partitions:1}")
    private int watermarkTopicPartitions;

    @Value("${app.kafka.watermark-topic-replication-factor:3}")
    private short watermarkTopicReplicationFactor;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public NewTopic dlqNewTopic() {
        return TopicBuilder.name(dlqTopic)
                .partitions(dlqPartitions)
                .replicas(dlqReplicationFactor)
                .build();
    }

    @Bean
    public NewTopic lateNewTopic() {
        return TopicBuilder.name(lateTopic)
                .partitions(lateTopicPartitions)
                .replicas(lateTopicReplicationFactor)
                .build();
    }

    @Bean
    public NewTopic watermarkNewTopic() {
        return TopicBuilder.name(watermarkTopic)
                .partitions(watermarkTopicPartitions)
                .replicas(watermarkTopicReplicationFactor)
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT)
                .build();
    }

    @Bean
    public KafkaTemplate<String, byte[]> dlqKafkaTemplate() {
        Map<String,Object> config = buildProducerConfig();
        ProducerFactory<String,byte[]> producerFactory = new DefaultKafkaProducerFactory<>(config);
        return new KafkaTemplate<>(producerFactory);
    }

    private Map<String, Object> buildProducerConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        config.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        return config;
    }
}
