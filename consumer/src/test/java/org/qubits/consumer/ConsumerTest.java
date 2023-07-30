package org.qubits.consumer;

import org.apache.kafka.streams.Topology;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

class ConsumerTest {
  private final static String TEST_CONFIG_FILE = "configuration/test.properties";

  @Test
  public void topologyShouldSumValues() throws IOException {
    final Properties props = new Properties();
    try (InputStream inputStream = new FileInputStream(TEST_CONFIG_FILE)) {
      props.load(inputStream);
    }
    final String inputTopicName = props.getProperty("input.topic.name");
    final String outputTopicName = props.getProperty("output.topic.name");
    final Topology topology = Consumer.buildTopology(inputTopicName, outputTopicName);

  }

}