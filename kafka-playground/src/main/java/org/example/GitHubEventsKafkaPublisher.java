package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.*;
import java.net.URISyntaxException;
import java.util.Calendar;
import java.util.Properties;

public class GitHubEventsKafkaPublisher {
    private static final Integer maxCounter = 400000;

    public static void main(String[] args) throws IOException, URISyntaxException {
        String[] filePaths = prepareFiles();
        for (String filePath : filePaths) {
            publishFrom(filePath);
        }
    }

    private static void publishFrom(String filePath) throws IOException {
        KafkaProducer<String, String> producer = new KafkaProducer<>(getProperties());
        BufferedReader reader = new BufferedReader(new FileReader(filePath));
        String line;
        int counter = 0;
        while ((line = reader.readLine()) != null && (maxCounter == null || counter < maxCounter)) {
            ProducerRecord<String, String> record = new ProducerRecord<>(Configs.KAFKA_TOPIC_NAME, line);
            producer.send(record);
            counter++;
            if (counter % 1000 == 0) {
                System.out.printf("sent %d \tevents to kafka.\n", counter);
            }
        }
        producer.flush();
        reader.close();
    }

    private static String[] prepareFiles() throws IOException, URISyntaxException {
        Calendar now = Calendar.getInstance();
        int year = now.get(Calendar.YEAR);
        int month = now.get(Calendar.MONTH) + 1;
        int day = now.get(Calendar.DAY_OF_MONTH) - 1;
        int hour = now.get(Calendar.HOUR_OF_DAY) - 3;

        String filePath = GithubArchiveUtils.getGitHubArchiveFile(year, month, day, hour);

        return new String[]{filePath};
    }

    private static Properties getProperties() {
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Configs.KAFKA_BOOTSTRAP_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return properties;
    }
}
