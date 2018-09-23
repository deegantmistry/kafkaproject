package com.kafka.deegant.kafkaproject;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoWithThread {

	public static void main(String[] args) {
		// Consumer Demo
		new ConsumerDemoWithThread().run();
	}
	
	private ConsumerDemoWithThread() {
		
	}
	
	private void run() {
		
		Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class);
		String bootstrapServer = "localhost:9092";
		String topic = "new_topic";
		String consumerGroupId = "appeight";
		
		// latch for dealing with multiple threads
		CountDownLatch latch = new CountDownLatch(1);
		
		// create the consumer runnable
		logger.info("Creating the consumer thread...");
		Runnable myConsumerRunnable = new ConsumerRunnable(topic, bootstrapServer, consumerGroupId, latch);
		
		// start the thread
		Thread myThread = new Thread(myConsumerRunnable);
		myThread.start();

		 // add a shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread( () -> {
			logger.info("Caught shutdown hook.");
			((ConsumerRunnable) myConsumerRunnable).shutdown();
			try {
				latch.await();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			logger.info("Application has exited.");
		}));
		
		try {
			latch.await();
		} catch (InterruptedException e) {
			logger.error("Application got interrupted.", e);
		} finally {
			logger.info("Application is closing...");
		}
	}
	
	public class ConsumerRunnable implements Runnable {

		private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);
		private CountDownLatch latch;
		private KafkaConsumer<String, String> consumer;

		
		public ConsumerRunnable(String topic, String bootstrapServer, String consumerGroupId, CountDownLatch latch) {
			this.latch = latch;
			
			// create Consumer configs			
			Properties configs = new Properties();
			configs.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
			configs.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			configs.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			configs.setProperty(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
			configs.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

			// create KafkaConsumer
			consumer = new KafkaConsumer<String, String>(configs);
			// subscribe the consumer to topic(s)
			consumer.subscribe(Collections.singleton(topic));;

		}
		
		public void run() {			
			try {
				// poll for new data
				while(true) {  //only for testing
					ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));
					for (ConsumerRecord<String, String> record : records) {
						logger.info("Key: " + record.key());
						logger.info("Value: " + record.value());
						logger.info("Partition: " + record.partition());
						logger.info("Offset: " + record.offset());
						logger.info("Timestamp: " + record.timestamp());
					}			
				}				
			} catch (WakeupException e) {
				logger.info("some msg");
			} finally {
				// close the consumer to prevent resource leaks
				consumer.close();
				// tell the main code we are done with the consumer
				latch.countDown();
			}
			
		}
		
		public void shutdown() {
			
			// wakeup() method is a special method to interrupt consumer.poll()
			// it will throw an exception WakeUpException
			consumer.wakeup();
		}
		
	}
}
