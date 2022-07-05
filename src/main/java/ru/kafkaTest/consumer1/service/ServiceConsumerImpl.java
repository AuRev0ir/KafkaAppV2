package ru.kafkaTest.consumer1.service;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.stereotype.Service;


@EnableKafka
@Service
public class ServiceConsumerImpl {


    //Консюмеры SPRING BOOT  Х_х

    // Группа консюмеров(№1-3) с одинаковым GroupId


    @KafkaListener(topics = "spring-kafka-test", groupId = "groupVV1")
    public void  msgListener1(ConsumerRecord<Long,String> record){
//        System.out.printf("consumer 1: offset = %d, key = %s, value = %s, partition = %s%n",
//                record.offset(),
//                record.key(),
//                record.value(),
//                record.partition()
//        );
    }

    @KafkaListener(topics = "spring-kafka-test", groupId = "groupVV1")
    public void  msgListener2(ConsumerRecord<Long,String> record){
//        System.out.printf("consumer 2: offset = %d, key = %s, value = %s, partition = %s%n",
//                record.offset(),
//                record.key(),
//                record.value(),
//                record.partition()
//        );
    }

    @KafkaListener(topics = "spring-kafka-test", groupId = "groupVV1")
    public void  msgListener3(ConsumerRecord<Long,String> record){
//        System.out.printf("consumer 3: offset = %d, key = %s, value = %s, partition = %s%n",
//                record.offset(),
//                record.key(),
//                record.value(),
//                record.partition()
//        );
    }

    // Консюмер(№4) с другим GroupId

    @KafkaListener(topics = "spring-kafka-test", groupId = "groupVV2")
    public void  msgListener4(ConsumerRecord<Long,String> record){
//        System.out.printf("consumer 4: offset = %d, key = %s, value = %s, partition = %s%n",
//                record.offset(),
//                record.key(),
//                record.value(),
//                record.partition()
//        );
    }

    // Консюмер(№5) с подпиской на 1 и 2 партицию

    @KafkaListener(topics = "spring-kafka-test", groupId = " groupVV3",
            topicPartitions = @TopicPartition(topic = "spring-kafka-test",
                    partitions = {"1", "2"}))
    public void  msgListener5(ConsumerRecord<Long,String> record){
//        System.out.printf("consumer 5: offset = %d, key = %s, value = %s, partition = %s%n",
//                record.offset(),
//                record.key(),
//                record.value(),
//                record.partition()
//        );
    }

    // Повторное потребление сообщений при инициализации консюмера (№6)
    @KafkaListener(topics = "spring-kafka-test", groupId = "groupVV4",
            topicPartitions = @TopicPartition(topic = "spring-kafka-test", partitionOffsets ={
                    @PartitionOffset(partition = "0", initialOffset = "0"),
                    @PartitionOffset(partition = "1", initialOffset = "0"),
                    @PartitionOffset(partition = "2", initialOffset = "0")}))
    public void  msgListener6(ConsumerRecord<Long,String> record){
//        System.out.printf("consumer 6: offset = %d, key = %s, value = %s, partition = %s%n",
//                record.offset(),
//                record.key(),
//                record.value(),
//                record.partition()
//        );
    }

}
