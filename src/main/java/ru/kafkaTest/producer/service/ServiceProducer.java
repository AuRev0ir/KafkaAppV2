package ru.kafkaTest.producer.service;

public interface ServiceProducer {
    String sendOneMessage(Long msgId, String msg);
    String sendManyMessages(Long quantity, String msg);
    String sendManyMessagesByPartition(Integer partition, Long quantity, String msg);
    String sendOneMessageByPartition(Integer partition, Long msgId, String msg);

}
