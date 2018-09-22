package com.kafka.deegant.kafkaproject;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoWithCallback {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);
		String bootstrapServer = "localhost:9092";
		
		// create producer properties
		Properties configs = new Properties();
		configs.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		configs.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		configs.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		for (int i = 1; i <= 10; i++) {
			
			String topic = "new_topic";
			String key = "id_" + Integer.toString(i);
			String value = "hello from the java code!";

			// create ProducerRecord
			ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);
			logger.info("Key: " + key);
			
			// create KafkaProducer
			KafkaProducer<String, String> producer = new KafkaProducer<String, String>(configs);
			
			// send data
			producer.send(record, new Callback() {
				
				public void onCompletion(RecordMetadata metadata, Exception e) {
					// TODO Auto-generated method stub
					if (e == null) {
						// success
						logger.info("\nReceived message: " + "\n" +
								"Topic: " + metadata.topic() + "\n" +
								"Partition: " + metadata.partition() + "\n" +
								"Offset: " + metadata.offset() + "\n" + 
								"Timestamp: " + metadata.timestamp() + "\n"
						);
					}
					else {
						logger.error("Error while producing message." + e);
					}
				}
			});
			
			// flush and close producer
			producer.flush();
			producer.close();			
		}
		
	}

}
