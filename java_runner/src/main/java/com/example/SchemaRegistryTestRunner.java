package com.example;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.github.cdimascio.dotenv.Dotenv;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class SchemaRegistryTestRunner {

    public static void main(String[] args) throws IOException, InterruptedException {
        Dotenv dotenv = Dotenv.configure()
                .directory("../")
                .load();
        final String BOOTSTRAP_SERVERS = dotenv.get("BOOTSTRAP_SERVERS");
        System.out.println("BOOTSTRAP_SERVERS from env: " + BOOTSTRAP_SERVERS);

        final String SCHEMA_REGISTRY_URL = dotenv.get("SCHEMA_REGISTRY_URL");
        System.out.println("SCHEMA_REGISTRY_URL from env: " + SCHEMA_REGISTRY_URL);

        final String TOPIC = "user_avro";

        final String USER_AVRO_SCHEMA = "{" +
                "\"type\": \"record\"," +
                "\"name\": \"UserAvro\"," +
                "\"namespace\": \"com.example\"," +
                "\"fields\": [" +
                "{ \"name\": \"id\", \"type\": \"string\" }," +
                "{ \"name\": \"email\", \"type\": \"string\" }" +
                "]" +
                "}";

        List<Boolean> autoRegisterSchemasOptions = List.of(true, false);
        List<Boolean> useLatestVersionOptions = List.of(true, false);
        List<Boolean> useSchemaIdOptions = List.of(true, false);
        List<Boolean> idCompatibilityStrictOptions = List.of(true, false);
        List<String> valueSubjectNameStrategies = List.of(
                "io.confluent.kafka.serializers.subject.TopicNameStrategy",
                "io.confluent.kafka.serializers.subject.RecordNameStrategy",
                "io.confluent.kafka.serializers.subject.TopicRecordNameStrategy"
        );
        List<Boolean> valueSchemaProvidedOptions = List.of(true, false);

        PrintWriter csvWriter = new PrintWriter(new FileWriter("../test_results/schema_registry_test_results_java.csv"));
        csvWriter.println("auto_register_schemas,use_latest_version,use_schema_id,id_compatibility_strict,value_subject_name_strategy,value_schema_provided,successful_writes,errors");

        List<Runnable> tasks = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(96);

        for (boolean autoRegister : autoRegisterSchemasOptions) {
            for (boolean useLatest : useLatestVersionOptions) {
                for (boolean useSchemaId : useSchemaIdOptions) {
                    for (boolean idStrict : idCompatibilityStrictOptions) {
                        for (String strategy : valueSubjectNameStrategies) {
                            for (boolean valueSchemaProvided : valueSchemaProvidedOptions) {

                                Properties props = new Properties();
                                props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
                                props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
                                props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
                                props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
                                props.put("basic.auth.credentials.source", "USER_INFO");
                                props.put("schema.registry.basic.auth.user.info", dotenv.get("SR_API_KEY") + ":" + dotenv.get("SR_API_SECRET"));

                                props.put("auto.register.schemas", autoRegister);
                                props.put("use.latest.version", useLatest);
                                if (useSchemaId) {
                                    props.put("use.schema.id", 1);
                                }
                                props.put("id.compatibility.strict", idStrict);
                                props.put("value.subject.name.strategy", strategy);
                                props.put("max.block.ms", 10000); // 10 seconds

                                props.put("security.protocol", "SASL_SSL");
                                props.put("sasl.mechanism", "PLAIN");
                                props.put("sasl.jaas.config", String.format(
                                    "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
                                    dotenv.get("KAFKA_CLUSTER_API_KEY"), dotenv.get("KAFKA_CLUSTER_API_SECRET")
                                ));

                                tasks.add(() -> {
                                    Producer<String, Object> producer = new KafkaProducer<>(props);
                                    int success = 0;
                                    String error = "";
                                    try {
                                        Object record;
                                        if (valueSchemaProvided) {
                                            record = new org.apache.avro.generic.GenericData.Record(
                                                    new org.apache.avro.Schema.Parser().parse(USER_AVRO_SCHEMA)
                                            );
                                            ((org.apache.avro.generic.GenericData.Record) record).put("id", "123");
                                            ((org.apache.avro.generic.GenericData.Record) record).put("email", "test@example.com");
                                        } else {
                                            record = "invalid";
                                        }

                                        producer.send(new ProducerRecord<>(TOPIC, "key", record), (metadata, exception) -> {
                                            if (exception == null) {
                                                synchronized (csvWriter) {
                                                    csvWriter.println(String.format("%s,%s,%s,%s,%s,%s,1,",
                                                            autoRegister, useLatest, useSchemaId, idStrict,
                                                            strategy, valueSchemaProvided));
                                                }
                                            } else {
                                                String err = exception.getCause() != null ? exception.getCause().toString() : exception.toString();
                                                err = err.replaceAll("\n", " ").replaceAll(",", ";");
                                                synchronized (csvWriter) {
                                                    csvWriter.println(String.format("%s,%s,%s,%s,%s,%s,0,%s",
                                                            autoRegister, useLatest, useSchemaId, idStrict,
                                                            strategy, valueSchemaProvided, err));
                                                }
                                            }
                                            latch.countDown();
                                            producer.close();
                                        });
                                    } catch (Exception e) {
                                        String err = e.getCause() != null ? e.getCause().toString() : e.toString();
                                        err = err.replaceAll("\n", " ").replaceAll(",", ";");
                                        synchronized (csvWriter) {
                                            csvWriter.println(String.format("%s,%s,%s,%s,%s,%s,0,%s",
                                                    autoRegister, useLatest, useSchemaId, idStrict,
                                                    strategy, valueSchemaProvided, err));
                                        }
                                        latch.countDown();
                                        producer.close();
                                    }
                                });
                            }
                        }
                    }
                }
            }
        }

        for (Runnable task : tasks) {
            new Thread(task).start();
        }        
        latch.await();
        csvWriter.flush();
        csvWriter.close();
    }
}