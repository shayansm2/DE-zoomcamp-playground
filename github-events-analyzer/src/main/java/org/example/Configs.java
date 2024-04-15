package org.example;

public class Configs {
    public static final String KAFKA_BOOTSTRAP_SERVER = "localhost:9092";
    public static final String KAFKA_TOPIC_NAME = "github_events_v2";
    public static final String ELASTICSEARCH_SERVER_CLOUD = "https://de-zoomcamp-project2-elasticsearch.darkube.app";
    public static final String ELASTICSEARCH_SERVER_LOCAL = "localhost:9200";
    public static final String ELASTICSEARCH_USERNAME = "elastic";
    public static final String ELASTICSEARCH_PASSWORD = "qpaS680E8qmaR092p72T0wUoKtjCQSYB";
    public static final String ELASTICSEARCH_INDEX_EVENTS = "github_events";

    public enum Modes {
        LOCAL, CLOUD
    }

    public static final String MODE = String.valueOf(Modes.CLOUD);
}
