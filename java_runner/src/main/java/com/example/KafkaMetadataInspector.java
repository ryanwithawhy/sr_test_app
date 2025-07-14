package com.example;

import io.github.cdimascio.dotenv.Dotenv;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.ListTopicsResult;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaMetadataInspector {
    public static void main(String[] args) {
        Dotenv dotenv = Dotenv.configure()
                .directory("../")
                .load();

        String bootstrapServers = dotenv.get("BOOTSTRAP_SERVERS");
        String apiKey = dotenv.get("KAFKA_CLUSTER_API_KEY");
        String apiSecret = dotenv.get("KAFKA_CLUSTER_API_SECRET");

        Properties config = new Properties();
        config.put("bootstrap.servers", bootstrapServers);
        config.put("security.protocol", "SASL_SSL");
        config.put("sasl.mechanism", "PLAIN");
        config.put("sasl.jaas.config", String.format(
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
                apiKey, apiSecret
        ));

        // Required for multi-AZ DNS resolution
        config.put("client.dns.lookup", "use_all_dns_ips");

        // 3s timeout as requested
        config.put("request.timeout.ms", "3000");
        config.put("default.api.timeout.ms", "3000");

        try (AdminClient admin = AdminClient.create(config)) {
            DescribeClusterResult cluster = admin.describeCluster();
            System.out.println("Cluster ID: " + cluster.clusterId().get());
            System.out.println("Nodes: " + cluster.nodes().get());

            ListTopicsResult topics = admin.listTopics();
            System.out.println("Topics:");
            topics.names().get().forEach(System.out::println);
        } catch (ExecutionException | InterruptedException e) {
            System.err.println("❌ Error fetching metadata:");
            e.printStackTrace();
        }
    }
}
