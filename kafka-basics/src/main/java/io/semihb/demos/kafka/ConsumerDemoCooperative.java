package io.semihb.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

// Range assignor: the default partition assignment strategy.
// Cooperative sticky assignor: a new partition assignment strategy that is designed to be used with the new consumer's incremental rebalancing protocol.

public class ConsumerDemoCooperative {
    public static final Logger log = LoggerFactory.getLogger(ConsumerDemoCooperative.class.getSimpleName());

    public static void main(String[] args) {
        log.info("kafka consumer on semihb's machine...");

        String groupId = "semihb-kafka";
        String topic = "demo_java";

        Properties properties = new Properties();

        // consumer configs
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest"); // read from the beginning of the topic --beginning in cli
        properties.setProperty("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());

        // consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        //subscribe to a topic
        consumer.subscribe(Arrays.asList(topic));

        while (true) {
            log.info("Polling...");

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, String> record: records) {
                log.info("Key: " + record.key() + " value: " + record.value());
                log.info("Partition: " + record.partition() + " Offset: " + record.offset());
            }
        }

    }
}
