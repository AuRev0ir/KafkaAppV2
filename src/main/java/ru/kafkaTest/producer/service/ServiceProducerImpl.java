package ru.kafkaTest.producer.service;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import ru.kafkaTest.producer.exception.PartitionException;

@Service
public class ServiceProducerImpl implements ServiceProducer {

    @Value("${kafka.topicName}")
    private String topicName;

    @Value("${kafka.numberOfPartitions}")
    private int numberOfPartitions;
    private final KafkaTemplate<Long,String> kafkaTemplate;

    @Autowired
    public ServiceProducerImpl(KafkaTemplate<Long, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public String sendOneMessage(Long msgId, String msg) {
        ListenableFuture<SendResult<Long, String>> future = kafkaTemplate.send(topicName, msgId, msg);
        future.addCallback(System.out::println, System.err::println);
        kafkaTemplate.flush();
        return "Operation successful";
    }

    @Override
    public String sendManyMessages(Long quantity, String msg) {
        for(Long i = 0L; i <= quantity; i++) {
            kafkaTemplate.send(topicName, i, msg + " number: " + i);
            kafkaTemplate.flush();
        }
        return "Operation successful";
    }

    @Override
    public String sendManyMessagesByPartition(Integer partition, Long quantity, String msg) {
        if (partition <= numberOfPartitions - 1) {
            for(Long i = 1L; i <= quantity; i++) {
                kafkaTemplate.send(topicName, partition, i, msg +  " number: " + i);
                kafkaTemplate.flush();
            }
        } else{
            throw new PartitionException();
        }
        return "Operation successful";
    }

    @Override
    public String sendOneMessageByPartition(Integer partition, Long msgId, String msg) {
        if (partition <= numberOfPartitions - 1) {
            ListenableFuture<SendResult<Long, String>> future = kafkaTemplate.send(topicName,partition, msgId, msg);
            kafkaTemplate.flush();
        } else {
            throw new PartitionException();
        }
        return "Operation successful";
    }
}
