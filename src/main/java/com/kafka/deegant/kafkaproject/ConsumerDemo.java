package com.kafka.deegant.kafkaproject;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemo {

	public static void main(String[] args) {
		// Consumer Demo
		
		final Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);
		
		// create Consumer configs
		String bootstrapServer = "localhost:9092";
		String consumerGroupId = "appsix";
		String topic = "new_topic";
		
		Properties configs = new Properties();
		configs.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		configs.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		configs.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		configs.setProperty(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
		configs.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		// create KafkaConsumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(configs);
		
		// subscribe the consumer to topic(s)
		consumer.subscribe(Collections.singleton(topic));;
		
		// poll for new data
//		while(true) {  //only for testing
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));
			for (ConsumerRecord<String, String> record : records) {
				logger.info("Key: " + record.key());
				logger.info("Value: " + record.value());
				logger.info("Partition: " + record.partition());
				logger.info("Offset: " + record.offset());
				logger.info("Timestamp: " + record.timestamp());
			}			
//		}  // close while which was used only for testing
		

		// close the consumer to prevent resource leaks
		consumer.close();
	}

}
