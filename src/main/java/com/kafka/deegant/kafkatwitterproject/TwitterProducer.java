package com.kafka.deegant.kafkatwitterproject;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TwitterProducer {

	Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());
	String consumerKey = "consumerKey";
	String consumerSecretKey = "consumerSecretKey";
	String accessToken = "accessToken";
	String accessTokenSecret = "accessTokenSecret";
	List<String> terms = Lists.newArrayList("kafka");
	String propertyFile = "/twitterproducer/configs.properties";

	
	public TwitterProducer() {}
	
	public static void main(String[] args) {
		new TwitterProducer().run();
	}
	
	public void run() {
		
		/** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

		// create twitter client
		Client client = createTwitterClient(msgQueue);
		
		// Attempts to establish a connection.
		client.connect();
		
		// create kafka producer
		KafkaProducer<String, String> producer = createKafkaProducer();		
		
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			logger.info("Received shutdown hook.");
			client.stop();
			producer.close();
		}));
		
		// in a loop - send tweets to kafka
		// on a different thread, or multiple different threads....
		while (!client.isDone()) {
			String msg = null;
			try {
				msg = msgQueue.poll(5, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				client.stop();
			}
			
			if(msg != null) {
//				logger.info(msg);
				producer.send(new ProducerRecord<String, String>("twitter-tweets", msg), new Callback() {
					
					@Override
					public void onCompletion(RecordMetadata metadata, Exception e) {
						if(e != null) {
							logger.error("Something bad happened!", e);
						}
					}
				});
			}
			
		}
		logger.info("End of application.");

	}
	
	private KafkaProducer<String, String> createKafkaProducer() {

		Properties inputProps = new Properties();
		InputStream s = null;
		
		try {
			s = new FileInputStream(propertyFile);
			inputProps.load(s);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		// load props ///////
		String BOOTSTRAP_SERVERS_CONFIG = inputProps.getProperty("BOOTSTRAP_SERVERS_CONFIG");
		String ENABLE_IDEMPOTENCE_CONFIG = inputProps.getProperty("ENABLE_IDEMPOTENCE_CONFIG"); 
		String ACKS_CONFIG = inputProps.getProperty("ACKS_CONFIG"); 
		String MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION = inputProps.getProperty("MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION"); 
		String COMPRESSION_TYPE_CONFIG = inputProps.getProperty("COMPRESSION_TYPE_CONFIG");
		/////////////////////
		
		
		// create producer properties
		Properties kafkaConfigs = new Properties();
		kafkaConfigs.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS_CONFIG);
		kafkaConfigs.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		kafkaConfigs.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		// add properties for the producer to make it a safe producer
		kafkaConfigs.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, ENABLE_IDEMPOTENCE_CONFIG);
		kafkaConfigs.setProperty(ProducerConfig.ACKS_CONFIG, ACKS_CONFIG);
		kafkaConfigs.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
		kafkaConfigs.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION);
		
		// properties for high throughput (at the expense of a little latency and some CPU cycles)
		kafkaConfigs.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, COMPRESSION_TYPE_CONFIG);
		kafkaConfigs.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024)); // 32KB batch size
		kafkaConfigs.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
		
		KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaConfigs);
		return producer;
	}

	public Client createTwitterClient(BlockingQueue<String> msgQueue) {

		/** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		// Optional: set up some followings and track terms
		List<Long> followings = Lists.newArrayList(1234L, 566788L);
		hosebirdEndpoint.followings(followings);
		hosebirdEndpoint.trackTerms(terms);

		// These secrets should be read from a config file
		Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecretKey, accessToken, accessTokenSecret);
		
		ClientBuilder builder = new ClientBuilder()
				  .name("Hosebird-Client-01")                              // optional: mainly for the logs
				  .hosts(hosebirdHosts)
				  .authentication(hosebirdAuth)
				  .endpoint(hosebirdEndpoint)
				  .processor(new StringDelimitedProcessor(msgQueue));

		Client hosebirdClient = builder.build();
		return hosebirdClient;

	}
}
