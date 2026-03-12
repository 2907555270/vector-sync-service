package com.example.vectorsync.service;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.WriteResponseBase;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.elasticsearch.core.bulk.DeleteOperation;
import co.elastic.clients.elasticsearch.core.bulk.IndexOperation;
import com.example.vectorsync.config.SyncProperties;
import com.example.vectorsync.model.SyncMessage;
import com.example.vectorsync.model.VectorDocument;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class ElasticsearchService {

    private final ElasticsearchClient esClient;
    private final SyncProperties syncProperties;
    private final VectorTransformService vectorTransformService;

    public BulkResponse bulkIndex(List<SyncMessage> messages) throws IOException {
        List<BulkOperation> operations = new ArrayList<>();

        List<SyncMessage> toCreateOrUpdate = messages.stream()
                .filter(SyncMessage::isCreateOrUpdate)
                .collect(Collectors.toList());

        List<SyncMessage> toDelete = messages.stream()
                .filter(SyncMessage::isDelete)
                .collect(Collectors.toList());

        for (SyncMessage message : toCreateOrUpdate) {
            VectorDocument doc = vectorTransformService.transform(message);
            IndexOperation<VectorDocument> indexOp = IndexOperation.of(op -> op
                    .index(syncProperties.getElasticsearch().getIndex())
                    .id(message.getId())
                    .document(doc)
            );
            operations.add(BulkOperation.of(b -> b.index(indexOp)));
        }

        for (SyncMessage message : toDelete) {
            DeleteOperation deleteOp = DeleteOperation.of(op -> op
                    .index(syncProperties.getElasticsearch().getIndex())
                    .id(message.getId())
            );
            operations.add(BulkOperation.of(b -> b.delete(deleteOp)));
        }

        if (operations.isEmpty()) {
            log.debug("No operations to execute");
            return BulkResponse.of(r -> r
                    .took(0)
                    .errors(false)
                    .items(List.of())
            );
        }

        BulkRequest bulkRequest = BulkRequest.of(br -> br.operations(operations));
        BulkResponse response = esClient.bulk(bulkRequest);

        if (response.errors()) {
            log.error("Bulk operation had errors");
            for (BulkResponseItem item : response.items()) {
                if (item.error() != null) {
                    log.error("Error for document {}: {}", item.id(), item.error().reason());
                }
            }
        } else {
            log.info("Successfully indexed {} documents, deleted {} documents",
                    toCreateOrUpdate.size(), toDelete.size());
        }

        return response;
    }

    public WriteResponseBase indexDocument(VectorDocument document) throws IOException {
        IndexResponse response = esClient.index(idx -> idx
                .index(syncProperties.getElasticsearch().getIndex())
                .id(document.getId())
                .document(document)
        );
        log.debug("Indexed document: {}", document.getId());
        return response;
    }

    public WriteResponseBase deleteDocument(String id) throws IOException {
        DeleteResponse response = esClient.delete(del -> del
                .index(syncProperties.getElasticsearch().getIndex())
                .id(id)
        );
        log.debug("Deleted document: {}", id);
        return response;
    }

    public Map<String, Object> getDocument(String id) throws IOException {
        GetResponse<VectorDocument> response = esClient.get(get -> get
                        .index(syncProperties.getElasticsearch().getIndex())
                        .id(id),
                VectorDocument.class
        );
        if (response.found()) {
            return Map.of("id", response.id(), "source", response.source());
        }
        return Map.of();
    }

    public boolean exists(String id) throws IOException {
        return esClient.exists(ex -> ex
                .index(syncProperties.getElasticsearch().getIndex())
                .id(id)
        ).value();
    }

    public long count() throws IOException {
        return esClient.count(cnt -> cnt
                .index(syncProperties.getElasticsearch().getIndex())
        ).count();
    }
}
