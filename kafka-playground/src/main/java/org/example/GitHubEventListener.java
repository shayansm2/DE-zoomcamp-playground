package org.example;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.time.Duration;
import java.util.*;

public class GitHubEventListener {
    private static final String ConsumerGroupName = "group-test-2";

    public static void main(String[] args) throws IOException {
        KafkaConsumer<String, String> consumer = getKafkaConsumer();
        ElasticsearchClient esClient = getElasticSearchClient();

        int counter = 0;
        ConsumerRecords<String, String> records;
        while ((records = consumer.poll(Duration.ofSeconds(1))) != null) {
            for (ConsumerRecord<String, String> record : records) {
                counter++;

                GitHubEventData eventData = new GitHubEventData(record.value());

                esClient.index(i -> i
                        .index(Configs.ELASTICSEARCH_INDEX_EVENTS)
                        .id(eventData.id)
                        .document(eventData)
                );

                if (counter % 100 == 0) {
                    System.out.printf("indexed %d\t records\n", counter);
                }
            }
        }
    }

    private static KafkaConsumer<String, String> getKafkaConsumer() {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(getProperties());
        consumer.subscribe(List.of(Configs.KAFKA_TOPIC_NAME));
        return consumer;
    }

    private static ElasticsearchClient getElasticSearchClient() {
        RestClient restClient = RestClient.builder(HttpHost.create(Configs.ELASTICSEARCH_SERVER)).build();
        ElasticsearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
        return new ElasticsearchClient(transport);
    }

    public static Properties getProperties() {
        Properties properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Configs.KAFKA_BOOTSTRAP_SERVER);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, ConsumerGroupName);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return properties;
    }
}
