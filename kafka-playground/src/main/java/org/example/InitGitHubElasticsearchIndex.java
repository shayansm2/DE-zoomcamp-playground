package org.example;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.IndexSettings;

import java.io.IOException;

public class InitGitHubElasticsearchIndex {
    public static void main(String[] args) throws IOException {
        ElasticsearchClient esClient = ElasticSearchClient.get();
        IndexSettings settings = new IndexSettings.Builder()
//                .mapping(m -> m.totalFields(t -> t.limit(2000)))
                .numberOfReplicas("0")
                .build();

        CreateIndexRequest indexRequest = new CreateIndexRequest.Builder()
                .index(Configs.ELASTICSEARCH_INDEX_EVENTS)
                .settings(settings)
                .build();

        esClient.indices().create(indexRequest);
    }
}
