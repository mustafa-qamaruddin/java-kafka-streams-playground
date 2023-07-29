package producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.RoundRobinPartitioner;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.IntegerSerializer;

import java.util.Properties;
import java.util.Random;

@Slf4j
public class ProducerApp {
  public static final String KAFKA_TOPIC = "movies";

  public static void main(String[] args) {
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, DoubleSerializer.class.getName());
    properties.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, RoundRobinPartitioner.class.getName());
    KafkaProducer<Integer, Double> kafkaProducer = new KafkaProducer<>(properties);
    Random random = new Random(101);
    for (int i = 0; i < 3000; i++) {
      ProducerRecord<Integer, Double> producerRecord = new ProducerRecord<>(
        KAFKA_TOPIC,
        random.nextInt(11),
        random.nextDouble(5)
      );
      kafkaProducer.send(producerRecord,
        (RecordMetadata metadata, Exception exception) -> log.debug(
          "offset: {}, timestamp: {}, exception: {}", metadata.offset(), metadata.timestamp(), exception
        )
      );
    }
  }
}
