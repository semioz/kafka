package io.semihb.demos.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

//https://www.conduktor.io/kafka/producer-default-partitioner-and-sticky-partitioner/
/*
When key=null, the producer has a default partitioner that varies:
Round Robin: for Kafka 2.3 and below
Sticky Partitioner: for Kafka 2.4 and above

Sticky Partitioner improves the performance of the producer especially with high throughput.
*/

public class ProducerDemoKeys {
    public static final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());

    public static void main(String[] args) throws InterruptedException {
        log.info("kafka producer on semihb's machine...");

        // Create producer properties
        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // In first, it will be string but later than will be serialized into bytes
        // before being sent to Apache Kafka
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        properties.setProperty("batch.size", "400");
        // Sticky partitioner strategy: stick to a partition until the batch is closed
        // each partition receives a full batch

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int j = 0; j < 2; j++) {
            for (int i = 0; i < 10; i++) {
                String topic = "demo_java";
                String key = "id_" + i;
                String value = "hello semihb!" + i;
                // Create producer record
                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>(topic,key, value);
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        // executes every time a record successfully sent or an exception is thrown
                        if (e == null) {
                            log.info("Key: " + key + " | Partition: " + metadata.partition());
                        } else {
                            log.error("Error while producing: ", e);
                        }
                    }
                });
            }
        }

        // tell the producer to send all data and block until done --sync
        producer.flush();

        // flush and close the producer
        producer.close();
    }
}