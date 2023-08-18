package org.qubits.consumer;

import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.apache.kafka.streams.test.TestRecord;
import org.apache.kafka.streams.test.*;
import org.junit.jupiter.api.TestInstance;


import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.NoSuchElementException;
import java.util.Properties;

import static java.util.Objects.isNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ConsumerTest {
  private final static String TEST_CONFIG_FILE = "src/test/resources/configuration/test.properties";

  private TopologyTestDriver testDriver;
  private String inputTopicName;
  private String outputTopicName;

  @BeforeAll
  public void setup() throws IOException {
    // Setup topology
    final Properties topologyProps = new Properties();
    try (InputStream inputStream = new FileInputStream(TEST_CONFIG_FILE)) {
      topologyProps.load(inputStream);
    }
    inputTopicName = topologyProps.getProperty("input.topic.name");
    outputTopicName = topologyProps.getProperty("output.topic.name");
    final Topology topology = Consumer.buildTopology(inputTopicName, outputTopicName);

    // Setup test driver
    Properties streamProps = new Properties();
    streamProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
    streamProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
    streamProps.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
    streamProps.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Double().getClass().getName());

    testDriver = new TopologyTestDriver(topology, streamProps);
  }

  @AfterAll
  public void tearDown() {
    // Teardown
    testDriver.close();
  }

  @Test
  public void topologyShouldSumValues() {
    // Given
    IntegerSerializer integerSerializer = new IntegerSerializer();
    DoubleSerializer doubleSerializer = new DoubleSerializer();
    TestInputTopic<Integer, Double> testInputTopic = testDriver.createInputTopic(
      inputTopicName, integerSerializer, doubleSerializer
    );

    IntegerDeserializer integerDeserializer = new IntegerDeserializer();
    DoubleDeserializer doubleDeserializer = new DoubleDeserializer();
    TestOutputTopic<Integer, Double> testOutputTopic = testDriver.createOutputTopic(
      outputTopicName, integerDeserializer, doubleDeserializer
    );

    // When
    testInputTopic.pipeInput(new TestRecord<>(1, 3.5));
    testInputTopic.pipeInput(new TestRecord<>(2, 4.0));
    testInputTopic.pipeInput(new TestRecord<>(1, 1.5));
    testInputTopic.pipeInput(new TestRecord<>(1, 4.5));

    // Then
    assertEquals(testOutputTopic.readKeyValue(), new KeyValue<>(1, 3.5));
    assertEquals(testOutputTopic.readKeyValue(), new KeyValue<>(2, 4.0));
    assertEquals(testOutputTopic.readKeyValue(), new KeyValue<>(1, 5.0));
    assertEquals(testOutputTopic.readKeyValue(), new KeyValue<>(1, 9.5));
    assertThrows(NoSuchElementException.class, () -> testOutputTopic.readKeyValue());
    assertTrue(testOutputTopic.isEmpty());
  }

}