package com.kafka.deegant.kafkatwitterproject;

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
		String bootstrapServer = "localhost:9092";
		
		// create producer properties
		Properties configs = new Properties();
		configs.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		configs.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		configs.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		KafkaProducer<String, String> producer = new KafkaProducer<>(configs);
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
