package org.qubits.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

@Slf4j
public class Consumer {
  public static final String SOURCE_KAFKA_TOPIC = "movies";
  public static final String TARGET_KAFKA_TOPIC = "movies-enriched";
  public static final String CONSUMER_GROUP = "cg-101";

  public static void main(String[] args) {
    Properties properties = new Properties();
    properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "java-kafka-stream-playground-app-v2");
    properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
    properties.setProperty(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "2");

    // You can specify parameters for the Kafka consumers, producers, and admin client that are used internally.
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP);
    properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");

    Topology topology = buildTopology(SOURCE_KAFKA_TOPIC, TARGET_KAFKA_TOPIC);

    KafkaStreams kafkaStreams = new KafkaStreams(topology, properties);

    // This handler is called whenever a stream thread is terminated by an unexpected exception
    kafkaStreams.setUncaughtExceptionHandler((Thread thread, Throwable throwable) -> {
      log.error(throwable.getMessage());
    });

    // To allow your application to gracefully shutdown in response to SIGTERM
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
      // Rolling aggregation. Combines the values of (non-windowed) records by the grouped key. The current record value
      // is combined with the last reduced value, and a new reduced value is returned. The result value type cannot be
      // changed, unlike aggregate.
      .reduce(
        // an “adder” aggregator (e.g., aggValue + curValue).
        (aggValue, newValue) -> aggValue + newValue
      )
      .toStream();

    averageStream.peek((k, v) -> log.warn("Observed event/averageStream: {}", v));

    // Outputs
    averageStream.to(targetKafkaTopic, Produced.with(Serdes.Integer(), Serdes.Double()));

    return builder.build();
  }
}
