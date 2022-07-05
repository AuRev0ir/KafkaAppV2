package ru.kafkaTest;


import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import ru.kafkaTest.consumer2.MyConsumer1;
import ru.kafkaTest.consumer2.MyConsumer2;

import java.time.Duration;
import java.util.*;

@SpringBootApplication
public class MainApplication {


	public static void main(String[] args) {
		SpringApplication.run(MainApplication.class, args);

        // 2 консюмера с автокомитом (№1-2)
		MyConsumer1 consumer1 = new MyConsumer1("groupV1");
		MyConsumer1 consumer2 = new MyConsumer1("groupV1");
		//


        // 2 консюмера с ручной подачей (№3-4) синхронная подача
		MyConsumer2 consumer3 = new MyConsumer2("groupV2");
		final int minBatchSizeConsumer3 = 200;
		List<ConsumerRecord<Long,String>> bufferConsumer3 = new ArrayList<>();

		MyConsumer2 consumer4 = new MyConsumer2("groupV2");
		final int minBatchSizeConsumer4 = 300;
		List<ConsumerRecord<Long,String>> bufferConsumer4 = new ArrayList<>();
        //

		// консюмер с ручной подачей (№5) асинхронная подача
		MyConsumer2 consumer5 = new MyConsumer2("groupV3");
		int minCommitSize = 10;
		int iCount = 0;

		//партиции
		TopicPartition topicPartition = new TopicPartition("spring-kafka-test",0);
		TopicPartition topicPartition1 = new TopicPartition("spring-kafka-test", 1);
		TopicPartition topicPartition2 = new TopicPartition("spring-kafka-test", 2);
        //

		//подписка на определенные партиции
		consumer1.getKafkaConsumer().assign(Arrays.asList(topicPartition,topicPartition1));
		consumer2.getKafkaConsumer().assign(List.of(topicPartition2));
		consumer3.getKafkaConsumer().assign(List.of(topicPartition));
		consumer4.getKafkaConsumer().assign(List.of(topicPartition));
		consumer5.getKafkaConsumer().assign(List.of(topicPartition));
        //



		try {
			while (true){
				ConsumerRecords<Long,String> records1 =
						consumer1.getKafkaConsumer().poll(Duration.ofMillis(100));
				for (ConsumerRecord<Long, String> record : records1) {
//					System.out.printf("consumer 1: offset = %d, key = %s, value = %s, partition = %s%n",
//							record.offset(), record.key(), record.value(), record.partition()
//					);
				}


				ConsumerRecords<Long,String> records2 =
						consumer2.getKafkaConsumer().poll(Duration.ofMillis(100));
				for (ConsumerRecord<Long, String> record : records2) {
//					System.out.printf("consumer 2: offset = %d, key = %s, value = %s, partition = %s%n",
//							record.offset(), record.key(), record.value(), record.partition()
//					);
				}


				ConsumerRecords<Long,String> records3 =
						consumer3.getKafkaConsumer().poll(Duration.ofMillis(100));
				for (ConsumerRecord<Long, String> record : records3) {
					bufferConsumer3.add(record);
//					System.out.printf("consumer 3: offset = %d, key = %s, value = %s, partition = %s%n",
//							record.offset(), record.key(), record.value(), record.partition()
//					);
				}
				if (bufferConsumer3.size() >= minBatchSizeConsumer3){
					try {
						consumer3.getKafkaConsumer().commitSync();
						bufferConsumer3.clear();
//						System.out.println("commit consumer 3");
					} catch (CommitFailedException e){
						e.printStackTrace();
					}
				}


				ConsumerRecords<Long,String> records4 =
						consumer4.getKafkaConsumer().poll(Duration.ofMillis(100));
				for (ConsumerRecord<Long, String> record : records4) {
					bufferConsumer4.add(record);
//					System.out.printf("consumer 4: offset = %d, key = %s, value = %s, partition = %s%n",
//							record.offset(), record.key(), record.value(), record.partition()
//					);
				}
				if (bufferConsumer4.size() >= minBatchSizeConsumer4){
					try {
						consumer4.getKafkaConsumer().commitSync();
						bufferConsumer4.clear();
//						System.out.println("commit consumer 4");
					} catch (CommitFailedException e){
						e.printStackTrace();
					}
				}


				try {
					ConsumerRecords<Long,String> records5 =
							consumer5.getKafkaConsumer().poll(Duration.ofMillis(100));
					for (ConsumerRecord<Long, String> record : records5) {
//						System.out.printf("consumer 5: offset = %d, key = %s, value = %s, partition = %s%n",
//								record.offset(), record.key(), record.value(), record.partition()
//						);
						iCount++;
					}
//					System.out.println("i count : "+iCount);
					if (iCount >= minCommitSize){
						consumer5.getKafkaConsumer().commitAsync(new OffsetCommitCallback() {
							@Override
							public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
								if (null == e) {
//									System.out.println("send successfully");
								} else {
//									System.out.println("an exception occurred");
								}
							}
						});
						iCount = 0;
					}
				} catch (Exception e){
					e.printStackTrace();
				}



	     	}
		} finally {
			consumer1.getKafkaConsumer().close();
			consumer2.getKafkaConsumer().close();
			consumer3.getKafkaConsumer().close();
			consumer4.getKafkaConsumer().close();
			consumer5.getKafkaConsumer().close();
		}
	}
}
