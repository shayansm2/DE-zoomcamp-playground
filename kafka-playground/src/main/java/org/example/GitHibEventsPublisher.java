package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class GitHibEventsPublisher {
    private static String bootstrapServer = "localhost:9092";
    private static String topicName = "test_topic";

    public static void main(String[] args) throws IOException {
        KafkaProducer<String, String> producer = new KafkaProducer<>(getProperties());
        BufferedReader reader = new BufferedReader(new FileReader("/Users/shayan/personal/DE-zoomcamp-playground/kafka-playground/files/2023-01-01-15.json"));
        String line;
        int counter = 0;
        while ((line = reader.readLine()) != null) {
            System.out.println(line);
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, line);
            producer.send(record);
            counter++;
            if (counter > 5) {
                break;
            }
        }
        producer.flush();
        System.out.println("DONE");
        reader.close();
    }

    private static Properties getProperties() {
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return properties;
    }
}
