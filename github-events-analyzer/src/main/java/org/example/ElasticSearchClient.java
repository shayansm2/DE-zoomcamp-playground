package org.example;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import java.util.Objects;

public class ElasticSearchClient {
    public static ElasticsearchClient get() {
        boolean isInCloud = Objects.equals(Configs.MODE, String.valueOf(Configs.Modes.CLOUD));
        String server = isInCloud ? Configs.ELASTICSEARCH_SERVER_CLOUD : Configs.ELASTICSEARCH_SERVER_LOCAL;
        RestClientBuilder builder = RestClient.builder(HttpHost.create(server));
        if (isInCloud) {
            builder.setHttpClientConfigCallback(httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider()));
        }
        RestClient restClient = builder.build();
        ElasticsearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
        return new ElasticsearchClient(transport);
    }

    private static CredentialsProvider credentialsProvider() {
        CredentialsProvider provider = new BasicCredentialsProvider();
        UsernamePasswordCredentials credentials = new UsernamePasswordCredentials(Configs.ELASTICSEARCH_USERNAME, Configs.ELASTICSEARCH_PASSWORD);
        provider.setCredentials(AuthScope.ANY, credentials);
        return provider;
    }
}
