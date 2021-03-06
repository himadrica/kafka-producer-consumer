package com.solvright.streamkafka;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import com.google.gson.JsonParser;

public class TweetsFilter {

	public static void main(String[] args) {
		// Create properties
		Properties properties = new Properties();
		properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-application-stream");
		properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
		properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

		// create a topology
		StreamsBuilder streamBuilder = new StreamsBuilder();
		// input topic
		KStream<String, String> allTweets = streamBuilder.stream("twitter_tweets");
		KStream<String, String> filteredTweets = allTweets.filter((K, jsonTweet) -> extractFollwers(jsonTweet) > 1000);
		filteredTweets.to("important_tweets");
		// build the topology
		KafkaStreams kafkaStreams = new KafkaStreams(streamBuilder.build(), properties);
		// start our streams application
		kafkaStreams.start();

	}

	private static int extractFollwers(String value) {
		 try {
			return JsonParser.parseString(value).getAsJsonObject().get("user").getAsJsonObject().get("followers_count")
					.getAsInt();
		} catch (Exception e) {
			return 0;
		}
	}

}
