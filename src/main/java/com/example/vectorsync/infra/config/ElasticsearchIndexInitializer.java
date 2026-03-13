package com.example.vectorsync.infra.config;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.ExistsRequest;
import co.elastic.clients.elasticsearch.indices.GetIndexRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class ElasticsearchIndexInitializer implements ApplicationRunner {

    private final ElasticsearchClient esClient;

    @Value("${sync.elasticsearch.index:nl2sql_vectors}")
    private String indexName;

    @Value("${sync.vector.dimension:384}")
    private int vectorDimension;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        log.info("Initializing Elasticsearch index: {}", indexName);

        boolean exists = esClient.indices()
                .exists(ExistsRequest.of(e -> e.index(indexName)))
                .value();

        if (!exists) {
            createIndex();
            log.info("Index {} created successfully", indexName);
        } else {
            log.info("Index {} already exists, checking mapping...", indexName);
            checkMapping();
        }
    }

    private void createIndex() throws Exception {
        CreateIndexRequest request = CreateIndexRequest.of(c -> c
                .index(indexName)
                .mappings(m -> m
                        .properties("id", p -> p.keyword(k -> k))
                        .properties("content", p -> p.text(t -> t))
                        .properties("dense_vector", p -> p.denseVector(dv -> dv
                                .dims(vectorDimension)
                                .index(true)
                                .similarity(co.elastic.clients.elasticsearch._types.Similarity.Cosine)))
                        .properties("sparse_vector", p -> p.sparseVector(sv -> sv)))
                .settings(s -> s
                        .numberOfShards("1")
                        .numberOfReplicas("1"))
        );

        esClient.indices().create(request);
    }

    private void checkMapping() throws Exception {
        var response = esClient.indices()
                .get(GetIndexRequest.of(g -> g.index(indexName)));

        var indexMapping = response.get(indexName).mappings();
        if (indexMapping != null && indexMapping.properties() != null) {
            var properties = indexMapping.properties();

            if (!properties.containsKey("sparse_vector")) {
                log.warn("Index {} is missing sparse_vector field. Consider reindexing.", indexName);
            } else {
                var sparseVectorField = properties.get("sparse_vector");
                String actualType = sparseVectorField.isObject()
                        ? sparseVectorField.asObject().asMap().get("type").toString()
                        : "unknown";
                log.info("sparse_vector field type: {}", actualType);

                if (!"sparse_vector".equals(actualType)) {
                    log.error("sparse_vector field type is incorrect! Expected: sparse_vector, Actual: {}",
                            actualType);
                }
            }
        }
    }
}
