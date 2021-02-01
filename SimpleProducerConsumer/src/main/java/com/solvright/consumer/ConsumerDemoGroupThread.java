package com.solvright.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoGroupThread {
	public static Logger logger = LoggerFactory.getLogger(ConsumerDemoGroupThread.class);

	public static void main(String[] args) {
		String bootstrapServer = "127.0.0.1:9092";
		String topic = "demo_topic";

		// create consumer config
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		CountDownLatch latch = new CountDownLatch(1);
		logger.info("Thread is starting.... ");
		ConsumerThread myThread = new ConsumerThread(topic, properties, latch);
		new Thread(myThread).start();

		// add shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			logger.info("Catch shutdown hook");
			myThread.shutdown();
			try {
				latch.await();
			} catch (InterruptedException e) {
				logger.info("Application has exited");
			}
		}));

		try {
			latch.await();
		} catch (InterruptedException e) {
			logger.info("Application is inturrupted", e);
		} finally {
			logger.info("Application is closing");
		}

	}

}

class ConsumerThread implements Runnable {
	private Logger logger = LoggerFactory.getLogger(ConsumerThread.class);
	private CountDownLatch latch;
	private String topic;
	// create consumer
	KafkaConsumer<String, String> consumer;

	public ConsumerThread(String topic, Properties properties, CountDownLatch latch) {
		this.topic = topic;
		this.latch = latch;
		consumer = new KafkaConsumer<String, String>(properties);
	}

	@Override
	public void run() {

		// assign and seek are mostly used to replay data or fetch a specific message
		
		// assign
		TopicPartition partitionToReadFrom = new TopicPartition(topic,0);
		long offsetToReadFrom = 15l;
		consumer.assign(Arrays.asList(partitionToReadFrom));
		
		// seek
		
		consumer.seek(partitionToReadFrom, offsetToReadFrom);
		// pull new data
		
		try {
			int numberOfMessageToRead = 5;
			boolean keepOnReading = true;
			int numberOfMessagesReadSoFar = 0;
			
			while (keepOnReading) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

				for (ConsumerRecord<String, String> record : records) {
					numberOfMessagesReadSoFar +=1;
					logger.info("key: " + record.key() + ", value: " + record.value());
					logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
					if(numberOfMessagesReadSoFar >= numberOfMessageToRead) {
						keepOnReading = false;
						break;
					}
				}
			}
			logger.info("Message read finished");
			
		} catch (WakeupException e) {
			logger.info("Received shutdown signal");
		} finally {
			consumer.close();
			// tell our main code we are done!
			latch.countDown();
		}

	}

	public void shutdown() {
		consumer.wakeup();
	}

}