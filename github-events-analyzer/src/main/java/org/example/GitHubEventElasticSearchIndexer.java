package org.example;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.JSONException;

import java.io.IOException;
import java.time.Duration;
import java.util.*;

public class GitHubEventElasticSearchIndexer {
    private static final String ConsumerGroupName = "group-test-3";

    public static void main(String[] args) throws IOException {
        KafkaConsumer<String, String> consumer = getKafkaConsumer();
        ElasticsearchClient esClient = ElasticSearchClient.get();

        int counter = 0;
        ConsumerRecords<String, String> records;
        while ((records = consumer.poll(Duration.ofSeconds(1))) != null) {
            BulkRequest.Builder builder = new BulkRequest.Builder();
            for (ConsumerRecord<String, String> record : records) {
                counter++;
                GitHubEventData eventData;
                try {
                    eventData = new GitHubEventData(record.value());
                } catch (JSONException exception) {
                    System.out.println(STR."count not send this event: \{record.value()}");
                    continue;
                }

                builder.operations(op -> op
                        .index(i -> i
                                .index(Configs.ELASTICSEARCH_INDEX_EVENTS)
                                .id(eventData.id)
                                .document(eventData)
                        )
                );
            }
            if (records.isEmpty()) {
                continue;
            }
            BulkRequest request = builder.build();
            esClient.bulk(request);
            System.out.printf("indexed %d\t records\n", counter);
        }
    }

    private static KafkaConsumer<String, String> getKafkaConsumer() {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(getProperties());
        consumer.subscribe(List.of(Configs.KAFKA_TOPIC_NAME));
        return consumer;
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
