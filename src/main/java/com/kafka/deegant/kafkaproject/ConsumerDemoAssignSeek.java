package com.kafka.deegant.kafkaproject;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoAssignSeek {

	public static void main(String[] args) {
		// Consumer Demo
		
		final Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class);
		
		// create Consumer configs
		String bootstrapServer = "localhost:9092";
		String topic = "new_topic";
		
		Properties configs = new Properties();
		configs.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		configs.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		configs.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		configs.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		// create KafkaConsumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(configs);
		
		// assign a topic partition
		TopicPartition partition = new TopicPartition(topic, 0);
		long offsetToReadFrom = 15L;
		
		// assign the partition to consumer
		consumer.assign(Collections.singleton(partition));
		
		consumer.seek(partition, offsetToReadFrom);
		
		int numOfMsgsToRead = 5;
		int numOfMsgsReadSoFar = 0;
		boolean keepOnReading = true;
		
		// poll for new data
		while(keepOnReading) {  //only for testing
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));
			for (ConsumerRecord<String, String> record : records) {
				numOfMsgsReadSoFar += 1;
				logger.info("Key: " + record.key());
				logger.info("Value: " + record.value());
				logger.info("Partition: " + record.partition());
				logger.info("Offset: " + record.offset());
				logger.info("Timestamp: " + record.timestamp());
				if (numOfMsgsReadSoFar >= numOfMsgsToRead) {
					keepOnReading = false;
					break;
				}
			}			
		}  // close while which was used only for testing
		
		logger.info("Exiting the application.");

		// close the consumer to prevent resource leaks
		consumer.close();
	}

}
