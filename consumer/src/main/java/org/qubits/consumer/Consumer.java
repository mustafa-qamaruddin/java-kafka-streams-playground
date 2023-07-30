package org.qubits.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

@Slf4j
public class Consumer {
  public static final String SOURCE_KAFKA_TOPIC = "movies";
  public static final String TARGET_KAFKA_TOPIC = "movies-enriched";

  public static void main(String[] args) {
    Properties properties = new Properties();
    properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "java-kafka-stream-playground-app-v2");
    properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
    properties.setProperty(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "2");

    Topology topology = buildTopology(SOURCE_KAFKA_TOPIC, TARGET_KAFKA_TOPIC);

    KafkaStreams kafkaStreams = new KafkaStreams(topology, properties);

    Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

    final CountDownLatch latch = new CountDownLatch(1);
    kafkaStreams.setStateListener((newState, oldState) -> {
      if (oldState == KafkaStreams.State.RUNNING && newState != KafkaStreams.State.RUNNING) {
        latch.countDown();
      }
    });

    kafkaStreams.start();

    try {
      latch.await();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public static Topology buildTopology(String sourceKafkaTopic, String targetKafkaTopic) {
    StreamsBuilder builder = new StreamsBuilder();
    // Inputs
    KStream<Integer, Double> inputStream = builder.stream(sourceKafkaTopic, Consumed.with(Serdes.Integer(), Serdes.Double()));

    inputStream.peek((k, v) -> log.warn("Observed event/inputStream: {}", v));

    // Processing
    KStream<Integer, Double> averageStream = inputStream
      .groupByKey()
      .aggregate(
        () -> 0.0,
        (key, value, aggregate) -> aggregate + value,
        Materialized.with(Serdes.Integer(), Serdes.Double())
      )
      .toStream();

    averageStream.peek((k, v) -> log.warn("Observed event/averageStream: {}", v));

    // Outputs
    averageStream.to(targetKafkaTopic, Produced.with(Serdes.Integer(), Serdes.Double()));

    Topology topology = builder.build();
    return topology;
  }
}
