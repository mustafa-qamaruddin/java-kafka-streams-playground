package org.qubits.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RoundRobinPartitioner;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Properties;

@Slf4j
public class Consumer {
  public static final String SOURCE_KAFKA_TOPIC = "movies";
  public static final String TARGET_KAFKA_TOPIC = "movies-enriched";

  public static void main(String[] args) {
    Properties properties = new Properties();
    properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "java-kafka-stream-playground-app");
    properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");

    StreamsBuilder builder = new StreamsBuilder();
    // Inputs
    KStream<Integer, Double> inputStream = builder.stream(SOURCE_KAFKA_TOPIC, Consumed.with(Serdes.Integer(), Serdes.Double()));
    // Processing
    KStream<Integer, Double> averageStream = inputStream
      .groupByKey()
      .aggregate(
        () -> 0.0,
        (key, value, aggregate) -> aggregate + value,
        Materialized.with(Serdes.Integer(), Serdes.Double())
      )
      .toStream();
    // Outputs
    averageStream.to(TARGET_KAFKA_TOPIC, Produced.with(Serdes.Integer(), Serdes.Double()));

    try (KafkaStreams streams = new KafkaStreams(builder.build(), properties)) {
      streams.start();

      Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
  }
}
