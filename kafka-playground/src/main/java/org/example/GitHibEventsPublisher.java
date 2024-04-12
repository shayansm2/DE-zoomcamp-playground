package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Calendar;
import java.util.Properties;
import java.util.zip.GZIPInputStream;

public class GitHibEventsPublisher {
    private static final Integer maxCounter = 400000;
    private static final int sleepTime = 1000;

    public static void main(String[] args) throws IOException, URISyntaxException {
        String filePath = downloadLastGitHubArchiveData();
        publishFrom(filePath);
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

    private static String downloadLastGitHubArchiveData() throws IOException, URISyntaxException {
        String fileName = getFileName();
        String filePath = String.format("./files/%s", fileName);
        Path path = Paths.get(filePath);
        if (Files.exists(path)) {
            return filePath;
        }

        String zipFilePath = download(fileName);
        unzip(zipFilePath, filePath);

        return filePath;
    }

    private static void unzip(String fromPath, String toPath) throws IOException {
        byte[] buffer = new byte[1024];
        GZIPInputStream gzis = new GZIPInputStream(new FileInputStream(fromPath));
        FileOutputStream out = new FileOutputStream(toPath);

        int len;
        while ((len = gzis.read(buffer)) > 0) {
            out.write(buffer, 0, len);
        }

        gzis.close();
        out.close();
        System.out.println("File decompressed successfully!");
    }

    private static String download(String fileName) throws IOException, URISyntaxException {
        URL urlObj = new URI(String.format("https://data.gharchive.org/%s.gz", fileName)).toURL();
        ReadableByteChannel channel = Channels.newChannel(urlObj.openStream());
        String zipFilePath = String.format("./files/%s.gz", fileName);

        FileOutputStream fOutStream = new FileOutputStream(zipFilePath);
        fOutStream.getChannel().transferFrom(channel, 0, Long.MAX_VALUE);
        System.out.println("File Successfully Downloaded from the URL and saved as: " + zipFilePath);
        return zipFilePath;
    }

    private static String getFileName() {
        Calendar now = Calendar.getInstance();
        return String.format(
                "%d-%02d-%02d-%d.json",
                now.get(Calendar.YEAR),
                now.get(Calendar.MONTH) + 1,
                now.get(Calendar.DAY_OF_MONTH) - 1,
                now.get(Calendar.HOUR_OF_DAY)
        );
    }

    private static Properties getProperties() {
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Configs.KAFKA_BOOTSTRAP_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return properties;
    }
}
