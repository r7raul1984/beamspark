package com.yhd.spark;

import com.google.common.collect.ImmutableMap;
import com.yhd.spark.oozie.OozieAuditLogKafkaDeserializer;
import com.yhd.spark.utils.EmbeddedKafkaCluster;
import com.yhd.spark.utils.PAssertStreaming;
import com.yhd.spark.utils.SparkTestPipelineOptionsForStreaming;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
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
        final String topic1 = "topic1";
        final String topic2 = "topic2";
        // messages.
        final Map<String, String> messages = ImmutableMap.of(
                "k1", "2016-04-27 15:01:14,526  INFO oozieaudit:520 - IP [192.168.7.199], USER [tangjijun], GROUP [pms], APP [My_Workflow], JOBID [0000000-160427140648764-oozie-oozi-W], " +
                        "OPERATION [start], PARAMETER [0000000-160427140648764-oozie-oozi-W], STATUS [SUCCESS], HTTPCODE [200], ERRORCODE [501], ERRORMESSAGE [no problem]"
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
        result.put("errorcode", "501");
        result.put("errormessage", "no problem");
        // write to both topics ahead.
        produce(topic1, messages);
        produce(topic2, messages);

        //------- test: read and dedup.
        Pipeline p = Pipeline.create(options);

        Map<String, Object> consumerProps = ImmutableMap.<String, Object>of(
                "auto.offset.reset", "earliest"
        );

        KafkaIO.Read<String, String> read = KafkaIO.<String, String>read()
                .withBootstrapServers(EMBEDDED_KAFKA_CLUSTER.getBrokerList())
                .withTopics(Arrays.asList(topic1, topic2))
                .withKeyCoder(StringUtf8Coder.of())
                .withValueCoder(StringUtf8Coder.of())
                .updateConsumerProperties(consumerProps);

        PCollection<KV<String, Map<String, String>>> deduped =

                p.apply(read.withoutMetadata()).setCoder(
                        KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
                        .apply(Window.<KV<String, String>>into(FixedWindows.of(batchIntervalDuration)))
                        .apply(Distinct.create())
                        .apply(ParDo.of(new ExtractLogFn()));


        // graceful shutdown will make sure first batch (at least) will finish.
        Duration timeout = Duration.standardSeconds(1L);
        PAssertStreaming.runAndAssertContents(p, deduped, expected, timeout);
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
            kafkaProducer.flush();
        }
    }

    private static Properties defaultProducerProps() {
        Properties producerProps = new Properties();
        producerProps.putAll(EMBEDDED_KAFKA_CLUSTER.getProps());
        producerProps.put("acks", "1");
        producerProps.put("bootstrap.servers", EMBEDDED_KAFKA_CLUSTER.getBrokerList());
        return producerProps;
    }


    private static class ExtractLogFn extends DoFn<KV<String, String>, KV<String, Map<String, String>>> {

        @ProcessElement
        public void processElement(ProcessContext c) throws UnsupportedEncodingException {

            String log = c.element().getValue();
            Map<String, String> map = (Map<String, String>) new OozieAuditLogKafkaDeserializer().deserialize(log.getBytes("UTF-8"));
            c.output(KV.of("f1", map));
        }
    }
}
