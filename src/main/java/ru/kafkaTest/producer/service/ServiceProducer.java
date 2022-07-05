package ru.kafkaTest.producer.service;

public interface ServiceProducer {
    void sendingOneMessage(Long msgId, String msg);
    void newsletterMsg (Long quantity, String msg);
    void newsletterMsgByPartition (int partition, Long quantity, String msg);

    void sendingOneMessageByPartition(int partition, Long msgId, String msg);

}
