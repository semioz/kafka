package io.semihb.demos.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {
    public static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("initializing kafka on semihb's machine...");

        // Create producer properties
        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // In first, it will be string but later than will be serialized into bytes
        // before being sent to Apache Kafka
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // Create producer record
        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>("demo_java", "launching kafka w/ semihb!");

        // send data
        producer.send(producerRecord);

        // tell the producer to send all data and block until done --sync
        producer.flush();

        // flush and close the producer
        producer.close();
    }
}
