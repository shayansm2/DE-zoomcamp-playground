package org.example;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.IndexSettings;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

import java.io.IOException;

public class InitGitHubElasticsearchIndex {
    public static void main(String[] args) throws IOException {
        ElasticsearchClient esClient = getElasticSearchClient();
        IndexSettings settings = new IndexSettings.Builder()
                .mapping(m -> m.totalFields(t -> t.limit(2000)))
                .build();
        CreateIndexRequest request = new CreateIndexRequest.Builder()
                .index(Configs.ELASTICSEARCH_INDEX_NAME)
                .settings(settings)
                .build();
        esClient.indices().create(request);
    }

    private static ElasticsearchClient getElasticSearchClient() {
        RestClient restClient = RestClient.builder(HttpHost.create(Configs.ELASTICSEARCH_SERVER)).build();
        ElasticsearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
        return new ElasticsearchClient(transport);
    }
}
