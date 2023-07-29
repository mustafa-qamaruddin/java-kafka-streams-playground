package org.qubits.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SlidingWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;

import java.time.Duration;
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

    StreamsBuilder builder = new StreamsBuilder();
    // Inputs
    KStream<Integer, Double> inputStream = builder.stream(SOURCE_KAFKA_TOPIC, Consumed.with(Serdes.Integer(), Serdes.Double()));

    inputStream.peek((k, v) -> log.warn("Observed event/inputStream: {}", v));

    // Processing
    KStream<Windowed<Integer>, Double> averageStream = inputStream
      .groupByKey()
      .windowedBy(SlidingWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(10)))
      .aggregate(
        () -> 0.0,
        (key, value, aggregate) -> aggregate + value,
        Materialized.with(Serdes.Integer(), Serdes.Double())
      )
      .toStream();

    averageStream.peek((k, v) -> log.warn("Observed event/averageStream: {}", v));

    // Outputs
    averageStream.to(TARGET_KAFKA_TOPIC, Produced.with(WindowedSerdes.timeWindowedSerdeFrom(Integer.TYPE, 10), Serdes.Double()));

    KafkaStreams streams = new KafkaStreams(builder.build(), properties);

    final CountDownLatch latch = new CountDownLatch(1);
    streams.setStateListener((newState, oldState) -> {
      if (oldState == KafkaStreams.State.RUNNING && newState != KafkaStreams.State.RUNNING) {
        latch.countDown();
      }
    });

    streams.start();

    try {
      latch.await();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
