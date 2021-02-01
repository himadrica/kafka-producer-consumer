package com.solvright.producer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerWithCallback {
	public static Logger logger = LoggerFactory.getLogger(ProducerWithCallback.class);

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		// kafka producer properties
		Properties kafkaProperties = new Properties();
		kafkaProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		kafkaProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		kafkaProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// create the prdoucer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(kafkaProperties);
		
		for( int i = 0 ; i < 10; i ++ ) {
		
			// create a producer record
			ProducerRecord<String, String> record = new ProducerRecord<String, String>("demo_topic",  "Hello world Abhik" + i);
			// send data
			producer.send(record, new Callback() {
	
				@Override
				public void onCompletion(RecordMetadata recordMetaData, Exception exception) {
					if(exception == null) {
						logger.info("Received new metadata, send successfully \n" + 
					                "Topic: " + recordMetaData.topic() + "\n" + 
									"Partision: " + recordMetaData.partition() + "\n" +
									"Offset: " + recordMetaData.offset() + "\n" +
									"Timestamp: " + recordMetaData.timestamp() + "\n");
					}else {
						logger.info("error producing message", exception);
					}
				}
	
			}).get();
		}
		producer.flush();
		producer.close();
	}

}
