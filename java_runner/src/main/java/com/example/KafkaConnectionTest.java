package com.example;

import io.github.cdimascio.dotenv.Dotenv;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaConnectionTest {

    public static void main(String[] args) {
        Dotenv dotenv = Dotenv.configure()
                .directory("../")
                .load();

        final String BOOTSTRAP_SERVERS = dotenv.get("BOOTSTRAP_SERVERS");
        final String TOPIC = "user_avro";

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put("max.block.ms", 3000); // 10 seconds

        System.out.println("Testing connection to broker at: " + BOOTSTRAP_SERVERS);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, "key", "test-connection");
            RecordMetadata metadata = producer.send(record).get();
            System.out.printf("✅ Message sent to topic %s partition %d offset %d%n",
                    metadata.topic(), metadata.partition(), metadata.offset());
        } catch (Exception e) {
            System.err.println("❌ Failed to connect or send message:");
            e.printStackTrace();
        }
    }
}
