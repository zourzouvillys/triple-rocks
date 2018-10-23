package zrz.triplerocks.replication;

import static org.apache.jena.graph.NodeFactory.createLiteral;
import static org.apache.jena.graph.NodeFactory.createURI;
import static org.junit.Assert.assertEquals;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Statement;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.ClassRule;
import org.junit.Test;

import com.salesforce.kafka.test.KafkaTestUtils;
import com.salesforce.kafka.test.junit4.SharedKafkaTestResource;

import zrz.triplediff.DeltaProcessor;

/**
 * performs a round trip test of triple synnchronization over kafka.
 * 
 * @author theo
 *
 */

public class TestKafka {

  private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(TestKafka.class);

  @ClassRule
  public static final SharedKafkaTestResource sharedKafkaTestResource = new SharedKafkaTestResource()
      .withBrokers(1)
      .withBrokerProperty("replication.factor", "1")
      .withBrokerProperty("min.insync.replicas", "1")
      .withBrokerProperty("offsets.topic.num.partitions", "1")
      .withBrokerProperty("offsets.topic.replication.factor ", "1")
      .withBrokerProperty("auto.create.topics.enable", "false");

  private KafkaTestUtils getKafkaTestUtils() {
    return sharedKafkaTestResource.getKafkaTestUtils();
  }

  @Test
  public void test() throws TimeoutException, InterruptedException {

    getKafkaTestUtils().waitForBrokerToComeOnLine(1, 5, TimeUnit.SECONDS);
    // getKafkaTestUtils().waitForBrokerToComeOnLine(2, 5, TimeUnit.SECONDS);

    final String topicName = "my-topic" + System.currentTimeMillis();

    getKafkaTestUtils().createTopic(topicName, 1, (short) 1);

    try (KafkaProducer<String, byte[]> p = sharedKafkaTestResource.getKafkaTestUtils().getKafkaProducer(StringSerializer.class, ByteArraySerializer.class)) {

      TripleRocksReplicationProducer producer = new TripleRocksReplicationProducer(data -> {

        log.info("sending delta of {} bytes", data.length);

        ProducerRecord<String, byte[]> record = new ProducerRecord<String, byte[]>(topicName, "change", data);

        try {
          Future<RecordMetadata> future = p.send(record);
          p.flush();
          while (!future.isDone()) {
            Thread.sleep(500L);
          }
          log.info("Produce completed, seq {}", future.get().offset());
        }
        catch (InterruptedException | ExecutionException e) {
          throw new RuntimeException(e);
        }

      });

      producer.insert(new Triple(
          createURI("testing"),
          createURI("propertyId"),
          createLiteral("my test value")));

      producer.sync();

      producer.insert(new Triple(
          createURI("testing"),
          createURI("anotherProperty"),
          createLiteral("my second value")));

      producer.insert(new Triple(
          createURI("testing2"),
          createURI("anotherProperty"),
          createLiteral("my second value for testing2")));

      producer.sync();

    }

    Model m = ModelFactory.createDefaultModel();
    TestDeltaStreamReceiver receiver = new TestDeltaStreamReceiver(m);

    Properties props = new Properties();
    try (final KafkaConsumer<String, byte[]> kafkaConsumer = getKafkaTestUtils().getKafkaConsumer(StringDeserializer.class, ByteArrayDeserializer.class,
        props)) {

      final List<TopicPartition> topicPartitionList = new ArrayList<>();

      for (final PartitionInfo partitionInfo : kafkaConsumer.partitionsFor(topicName)) {
        topicPartitionList.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
      }

      kafkaConsumer.assign(topicPartitionList);
      kafkaConsumer.seekToBeginning(topicPartitionList);

      // Pull records from kafka, keep polling until we get nothing back
      ConsumerRecords<String, byte[]> records;
      do {
        records = kafkaConsumer.poll(Duration.ofSeconds(1));
        log.info("Found {} records in kafka", records.count());
        for (ConsumerRecord<String, byte[]> record : records) {
          // Validate
          log.info("record seq {} in partition {}, topic {}", record.offset(), record.partition(), record.topic());
          DeltaProcessor.apply(record.value(), receiver);
        }
      }
      while (!records.isEmpty());

      kafkaConsumer.close(Duration.ofSeconds(2));

    }

    m.listStatements().forEachRemaining(stmt -> log.info("statement: {}", stmt));

    List<Statement> stmts = m.listStatements().toList();

    assertEquals(3, stmts.size());

  }

}
