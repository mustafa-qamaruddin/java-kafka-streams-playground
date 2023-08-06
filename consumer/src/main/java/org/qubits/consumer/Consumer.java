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

  public static final int NUM_THREADS = 2;

  public static void main(String[] args) {
    Properties properties = new Properties();
    properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "java-kafka-stream-playground-app-v2");
    properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
    // Each thread can execute one or more tasks with their processor topologies independently.
    // It is worth noting that there is no shared state amongst the threads, so no inter-thread coordination is necessary.
    properties.setProperty(StreamsConfig.NUM_STREAM_THREADS_CONFIG, String.format("%d", NUM_THREADS));
    // If tasks run on a machine that fails and are restarted on another machine, Kafka Streams guarantees to restore
    // their associated state stores to the content before the failure by replaying the corresponding changelog topics
    // prior to resuming the processing on the newly started tasks. As a result, failure handling is completely
    // transparent to the end user.
    // Note that the cost of task (re)initialization typically depends primarily on the time for restoring the state by
    // replaying the state stores' associated changelog topics. To minimize this restoration time, users can configure
    // their applications to have standby replicas of local states (i.e. fully replicated copies of the state). When a
    // task migration happens, Kafka Streams then attempts to assign a task to an application instance where such a
    // standby replica already exists in order to minimize the task (re)initialization cost. See num.standby.replicas in
    // the Kafka Streams Configs section.
    properties.setProperty(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, "3");

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

    // In addition, Kafka Streams makes sure that the local state stores are robust to failures, too. For each state store,
    // it maintains a replicated changelog Kafka topic in which it tracks any state updates. These changelog topics are
    // partitioned as well so that each local state store instance, and hence the task accessing the store, has its own
    // dedicated changelog topic partition.
    // TODO cause a failure so that the countDownLatch is tested
    final CountDownLatch latch = new CountDownLatch(NUM_THREADS);
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
