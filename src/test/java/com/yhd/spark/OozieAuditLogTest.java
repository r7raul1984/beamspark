package com.yhd.spark;

import com.google.common.collect.ImmutableMap;
import com.yhd.spark.oozie.OozieAuditLogKafkaDeserializer;
import com.yhd.spark.utils.EmbeddedKafkaCluster;
import com.yhd.spark.utils.SparkTestPipelineOptionsForStreaming;
import kafka.utils.ZKStringSerializer$;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.kafka8.Kafka8IO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.Duration;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

public class OozieAuditLogTest {
  private static final EmbeddedKafkaCluster.EmbeddedZookeeper EMBEDDED_ZOOKEEPER =
          new EmbeddedKafkaCluster.EmbeddedZookeeper();
  private static final EmbeddedKafkaCluster EMBEDDED_KAFKA_CLUSTER =
          new EmbeddedKafkaCluster(EMBEDDED_ZOOKEEPER.getConnection());

  @BeforeClass
  public static void init() throws IOException {
    EMBEDDED_ZOOKEEPER.startup();
    EMBEDDED_KAFKA_CLUSTER.startup();
  }

  @Rule
  public TemporaryFolder checkpointParentDir = new TemporaryFolder();

  @Rule
  public SparkTestPipelineOptionsForStreaming commonOptions =
          new SparkTestPipelineOptionsForStreaming();

  @Test
  public void testEarliest2Topics() throws Exception {
    Duration batchIntervalDuration = Duration.standardSeconds(5);
    SparkPipelineOptions options = commonOptions.withTmpCheckpointDir(checkpointParentDir);
    // provide a generous enough batch-interval to have everything fit in one micro-batch.
    options.setBatchIntervalMillis(batchIntervalDuration.getMillis());
    // provide a very generous read time bound, we rely on num records bound here.
    options.setMinReadTimeMillis(batchIntervalDuration.minus(1).getMillis());
    // bound the read on the number of messages - 2 topics of 4 messages each.
    options.setMaxRecordsPerBatch(8L);

    //--- setup
    // two topics.
    final String topic1 = "test";
    final String topic2 = "test";
    // messages.
    final Map<String, String> messages = ImmutableMap.of(
            "k1", "2016-04-27 15:01:14,526  INFO oozieaudit:520 - IP [192.168.7.199], USER [tangjijun], GROUP [pms], APP [My_Workflow], JOBID [0000000-160427140648764-oozie-oozi-W], " +
                    "OPERATION [start], PARAMETER [0000000-160427140648764-oozie-oozi-W], STATUS [777], HTTPCODE [200], ERRORCODE [502], ERRORMESSAGE [no problem]"
    );
    Map<String, String> result = new TreeMap<>();
    // expected.
    final KV<String, Map<String, String>>[] expected = new KV[]{KV.of("f1", result)};

    result.put("timestamp", "1461769274526");
    result.put("level", "INFO");
    result.put("ip", "192.168.7.199");
    result.put("user", "tangjijun");
    result.put("group", "pms");
    result.put("app", "My_Workflow");
    result.put("jobId", "0000000-160427140648764-oozie-oozi-W");
    result.put("operation", "start");
    result.put("parameter", "0000000-160427140648764-oozie-oozi-W");
    result.put("status", "SUCCESS");
    result.put("httpcode", "200");
    result.put("errorcode", "502");
    result.put("errormessage", "no problem");
    System.out.println(EMBEDDED_KAFKA_CLUSTER.getBrokerList());
    System.out.println(EMBEDDED_ZOOKEEPER.getConnection());
    // makeSureTopic();
    // write to both topics ahead.
    //  produce(topic1, messages);
    //  produce(topic2, messages);
    // Thread.sleep(2000);


    //------- test: read and dedup.
    Pipeline p = Pipeline.create(options);

    Map<String, String> consumerProps = ImmutableMap.<String, String>of(
            "auto.offset.reset", "smallest"
    );

    Kafka8IO.Read<String, String> read = Kafka8IO.<String, String>read()
            //EMBEDDED_KAFKA_CLUSTER.getBrokerList()
            .withBootstrapServers("sandbox.hortonworks.com:6667")
            .withTopics(Arrays.asList("test"))
            .withKeyCoder(StringUtf8Coder.of())
            .withValueCoder(StringUtf8Coder.of())
            .updateKafkaClusterProperties(consumerProps);
    PCollection<KV<String, Map<String, String>>> deduped =
            p.apply(read.withoutMetadata())
                    .apply(ParDo.of(new ExtractLogFn()));
    //------- test: write.
    deduped.apply(Kafka8IO.<String, Map<String, String>>write()
            .withBootstrapServers("sandbox.hortonworks.com:6667")
            .withTopic("oozie_audit_log_enriched")
            .withKeyCoder(StringUtf8Coder.of())
            .withValueCoder(MapCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
            .updateProducerProperties(ImmutableMap.of("bootstrap.servers", "sandbox.hortonworks.com:6667"))
    );
    p.run().waitUntilFinish();


    // graceful shutdown will make sure first batch (at least) will finish.
    // Duration timeout = Duration.standardSeconds(1L);
    // PAssertStreaming.runAndAssertContents(p, deduped, expected, timeout);
  }

  public static void makeSureTopic() {
    ZkClient zkClient = new ZkClient(EMBEDDED_ZOOKEEPER.getConnection(), 10000, 10000, ZKStringSerializer$.MODULE$);
    Properties topicConfiguration = new Properties();
    ZkConnection zkConnection = new ZkConnection(EMBEDDED_ZOOKEEPER.getConnection());
    // zkConnection.create()
    //ZkUtils zkUtils = new ZkUtils(zkClient, zkConnection, false);
    //  AdminUtils.createTopic(zkClient, "test", 1, 1, topicConfiguration);
    //  AdminUtils.createTopic(zkClient, "oozie_audit_log_enriched", 1, 1, topicConfiguration);
  }

  private static void produce(String topic, Map<String, String> messages) {
    Serializer<String> stringSerializer = new StringSerializer();
    try (@SuppressWarnings("unchecked") KafkaProducer<String, String> kafkaProducer =
                 new KafkaProducer(defaultProducerProps(), stringSerializer, stringSerializer)) {
      // feed topic.
      for (Map.Entry<String, String> en : messages.entrySet()) {
        kafkaProducer.send(new ProducerRecord<>(topic, en.getKey(), en.getValue()));
      }
      // await send completion.
      //  kafkaProducer.flush();
    }
  }

  private static Properties defaultProducerProps() {
    Properties producerProps = new Properties();
    // producerProps.putAll(EMBEDDED_KAFKA_CLUSTER.getProps());
    producerProps.put("acks", "1");
    producerProps.put("bootstrap.servers", "sandbox.hortonworks.com:6667");
    return producerProps;
  }


  private static class ExtractLogFn extends DoFn<KV<String, String>, KV<String, Map<String, String>>> {

    @ProcessElement
    public void processElement(ProcessContext c) throws UnsupportedEncodingException {

      String log = c.element().getValue();
      System.out.println("log" + log);
      Map<String, String> map = (Map<String, String>) new OozieAuditLogKafkaDeserializer().deserialize(log.getBytes("UTF-8"));
      c.output(KV.of("f1", map));
    }
  }
}
