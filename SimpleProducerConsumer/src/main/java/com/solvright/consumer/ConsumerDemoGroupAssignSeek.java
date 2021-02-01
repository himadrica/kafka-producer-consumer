package com.solvright.consumer;

import java.time.Duration;
import java.util.Arrays;
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

public class ConsumerDemoGroupAssignSeek {
	public static Logger logger = LoggerFactory.getLogger(ConsumerDemoGroupAssignSeek.class);

	public static void main(String[] args) {
		String bootstrapServer = "127.0.0.1:9092";
		String consumerGroup = "java-application-group";
		String topic = "demo_topic";

		// create consumer config
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		CountDownLatch latch = new CountDownLatch(1);
		logger.info("Thread is starting.... ");
		ConsumerThreadAssignAndSeek myThread = new ConsumerThreadAssignAndSeek(topic, properties, latch);
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

class ConsumerThreadAssignAndSeek implements Runnable {
	private Logger logger = LoggerFactory.getLogger(ConsumerThreadAssignAndSeek.class);
	private CountDownLatch latch;
	private String topic;
	// create consumer
	KafkaConsumer<String, String> consumer;

	public ConsumerThreadAssignAndSeek(String topic, Properties properties, CountDownLatch latch) {
		this.topic = topic;
		this.latch = latch;
		consumer = new KafkaConsumer<String, String>(properties);
	}

	@Override
	public void run() {

		// subscribe to a topic
		consumer.subscribe(Arrays.asList(topic));

		// pull new data
		try {
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

				for (ConsumerRecord<String, String> record : records) {
					logger.info("key: " + record.key() + ", value: " + record.value());
					logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
				}
			}
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