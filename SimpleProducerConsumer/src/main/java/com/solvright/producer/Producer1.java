package com.solvright.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class Producer1 {

	public static void main(String[] args) {
		// kafka producer properties
		Properties kafkaProperties = new Properties();
		kafkaProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		kafkaProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		kafkaProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// create the prdoucer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(kafkaProperties);

		// create a producer record
		ProducerRecord<String, String> record = new ProducerRecord<String, String>("demo_topic", "Hello Abhik java");
		// send data
		producer.send(record);
		producer.flush();
		producer.close();
	}
	
}
