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
    private static final String BOOTSTRAP_SERVER = "localhost:9092";
    private static final String TOPIC_NAME = "github_events";

    public static void main(String[] args) throws IOException {
        KafkaProducer<String, String> producer = new KafkaProducer<>(getProperties());
        BufferedReader reader = new BufferedReader(new FileReader(getFilePath()));
        String line;
        int counter = 0;
        int maxCounter = 2000;
        while ((line = reader.readLine()) != null && counter < maxCounter) {
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, line);
            producer.send(record);
            counter++;
            if (counter % 100 == 0) {
                System.out.printf("sent %d events to kafka.\n", counter);
            }
        }
        producer.flush();
        reader.close();
    }

    private static String getFilePath() {
        return "./files/2023-01-01-15.json";
    }

    private static Properties getProperties() {
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return properties;
    }
}
