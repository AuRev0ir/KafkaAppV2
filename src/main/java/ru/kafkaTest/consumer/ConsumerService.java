package ru.kafkaTest.consumer;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;


@EnableKafka
@Service
public class ConsumerService {


    @KafkaListener(topics = "kafka-test", groupId = "group1")
    public void  msgListener1(ConsumerRecord<Long,String> record){
        System.out.printf("consumer 1: offset = %d, key = %s, value = %s, partition = %s%n",
                record.offset(),
                record.key(),
                record.value(),
                record.partition()
        );
    }

    @KafkaListener(topics = "kafka-test", groupId = "group1")
    public void  msgListener2(ConsumerRecord<Long,String> record){
        System.out.printf("consumer 2: offset = %d, key = %s, value = %s, partition = %s%n",
                record.offset(),
                record.key(),
                record.value(),
                record.partition()
        );
    }

    @KafkaListener(topics = "kafka-test", groupId = "group1")
    public void  msgListener3(ConsumerRecord<Long,String> record){
        System.out.printf("consumer 3: offset = %d, key = %s, value = %s, partition = %s%n",
                record.offset(),
                record.key(),
                record.value(),
                record.partition()
        );
    }

}
