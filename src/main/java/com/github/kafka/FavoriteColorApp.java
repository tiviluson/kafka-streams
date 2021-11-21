package com.github.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;

public class FavoriteColorApp {
    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "favorite-colour");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> input = builder.stream("favorite-colour-input");

        // Assumption: producer sent key-value pairs
        final String[] valid_colors = {"green", "blue", "red"};
        KTable<String,Long> output = input
                .filter((user, color) -> Arrays.asList(valid_colors).contains(color))
                .map((user, color) -> KeyValue.pair(user.toLowerCase(), color.toLowerCase()))
                .toTable()
                .groupBy((user, color) -> KeyValue.pair(color, color))
                .count();

        output.toStream().to("favorite-colour-output");

        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, config);
        streams.cleanUp();
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
