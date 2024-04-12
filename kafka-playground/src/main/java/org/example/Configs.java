package org.example;

public class Configs {
    public static final String KAFKA_BOOTSTRAP_SERVER = "localhost:9092";
    public static final String KAFKA_TOPIC_NAME = "github_events";
    public static final String ELASTICSEARCH_SERVER_CLOUD = "localhost:9200";
    public static final String ELASTICSEARCH_SERVER_LOCAL = "localhost:9200";
    public static final String ELASTICSEARCH_USERNAME = "username";
    public static final String ELASTICSEARCH_PASSWORD = "password";
    public static final String ELASTICSEARCH_INDEX_EVENTS = "github_events";

    public enum Modes {
        LOCAL, CLOUD
    }

    public static final String MODE = String.valueOf(Modes.LOCAL);
}
