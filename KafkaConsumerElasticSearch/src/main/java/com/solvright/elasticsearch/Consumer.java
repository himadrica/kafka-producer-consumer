package com.solvright.elasticsearch;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonParser;

public class Consumer {
	public static Logger logger = LoggerFactory.getLogger(Consumer.class);

	public static void main(String[] args) throws IOException, InterruptedException {
		RestHighLevelClient client = createElasticSearchClient();

		// indexRequest.id("2");

		// create KAFKA consumer
		KafkaConsumer<String, String> kafkaConsumer = createKafkaConsumer("twitter_tweets");

		// pull new data
		while (true) {
			ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
			int numberOfRecords = records.count();
			logger.info("received " + numberOfRecords + " records");

			BulkRequest batchRequest = new BulkRequest();

			for (ConsumerRecord<String, String> record : records) {
				// There are two ways to make record idempotent
				// 1. using KAFKA topic partition and offset, id = record.topic() + "_" +
				// record.partition() + "_" + record.offset();
				// 2. using the record value unique id supplied by producer.
				String id = extractId(record.value()); // This is for idempotent behavior
				IndexRequest indexRequest = new IndexRequest("twitter");
				indexRequest.id(id);
				indexRequest.source(record.value(), XContentType.JSON);
				batchRequest.add(indexRequest);
				/*
				 //Record is inserting here one by one
				 IndexResponse response = client.index(indexRequest, RequestOptions.DEFAULT);
				 logger.info(response.getId());
				*/
			}
			if (numberOfRecords > 0) {
				BulkResponse batchResponse = client.bulk(batchRequest, RequestOptions.DEFAULT);
				logger.info("Committing offsets....");
				kafkaConsumer.commitSync();
				logger.info("Offsets have been committed");
				Thread.sleep(100);
			}
		}

	}

	public static RestHighLevelClient createElasticSearchClient() {
		// your own elastic configuration
		String hostname = "";
		String username = "";
		String password = "";
		//  https://3xxwjyrpc7:c3l8t1qn0y@kafka-playground-tes-4712029478.us-east-1.bonsaisearch.net:443
		final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
		credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

		RestClientBuilder builder = RestClient.builder(new HttpHost(hostname, 443, "https"))
				.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
					@Override
					public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
						return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
					}
				});
		RestHighLevelClient client = new RestHighLevelClient(builder);
		return client;
	}

	public static KafkaConsumer<String, String> createKafkaConsumer(String topic) {
		String bootstrapServer = "127.0.0.1:9092";
		String consumerGroup = "java-application-group";

		// create consumer config
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		// Disable auto commit
		properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");

		// create consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

		// subscribe to a topic
		consumer.subscribe(Arrays.asList(topic));

		return consumer;
	}

	private static String extractId(String value) {
		return JsonParser.parseString(value).getAsJsonObject().get("id_str").getAsString();
	}

}
