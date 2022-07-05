package ru.kafkaTest.topic;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.stereotype.Component;


@Configuration
public class TopicConfig {
    private final int PARTITION = 3;
    private final String TOPIC = "spring-kafka-test";


    @Bean
    public NewTopic topic(){
        return TopicBuilder
                .name(TOPIC)
                .partitions(PARTITION)
                .replicas(1)
                .build();
    }

    public int getPARTITION() {
        return PARTITION;
    }

    public String getTOPIC() {
        return TOPIC;
    }
}
