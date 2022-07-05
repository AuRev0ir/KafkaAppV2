package ru.kafkaTest.consumer2;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import ru.kafkaTest.topic.TopicConfig;

import java.util.Collections;
import java.util.Properties;

public class MyConsumer2 {
    private final String TOPIC = new TopicConfig().getTOPIC();
    private final KafkaConsumer<Long,String> kafkaConsumer;

    public MyConsumer2(String groupId) {

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        kafkaConsumer = new KafkaConsumer<>(props);
    }

    public KafkaConsumer<Long, String> getKafkaConsumer() {
        return kafkaConsumer;
    }
}
