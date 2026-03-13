package com.example.vectorsync.infra.service;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.WriteResponseBase;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.elasticsearch.core.bulk.DeleteOperation;
import co.elastic.clients.elasticsearch.core.bulk.IndexOperation;
import com.example.vectorsync.infra.config.SyncProperties;
import com.example.vectorsync.infra.model.SyncMessage;
import com.example.vectorsync.infra.model.VectorDocument;
import lombok.Getter;
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

    public BulkIndexResult bulkIndex(List<SyncMessage> messages) throws IOException {
        List<BulkOperation> operations = new ArrayList<>();

        List<SyncMessage> toCreateOrUpdate = messages.stream()
                .filter(SyncMessage::isCreateOrUpdate)
                .collect(Collectors.toList());

        List<SyncMessage> toDelete = messages.stream()
                .filter(SyncMessage::isDelete)
                .collect(Collectors.toList());

        for (SyncMessage message : toCreateOrUpdate) {
            try {
                VectorDocument doc = vectorTransformService.transform(message);
                IndexOperation<VectorDocument> indexOp = IndexOperation.of(op -> op
                        .index(syncProperties.getElasticsearch().getIndex())
                        .id(message.getId())
                        .document(doc)
                );
                operations.add(BulkOperation.of(b -> b.index(indexOp)));
            } catch (Exception e) {
                log.error("Failed to transform message {}: {}", message.getId(), e.getMessage());
                throw new IOException("Failed to transform message " + message.getId(), e);
            }
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
            BulkIndexResult result = new BulkIndexResult();
            result.setSuccess(true);
            result.setIndexedCount(0);
            result.setDeletedCount(0);
            return result;
        }

        BulkRequest bulkRequest = BulkRequest.of(br -> br.operations(operations));
        BulkResponse response = esClient.bulk(bulkRequest);

        BulkIndexResult result = new BulkIndexResult();
        result.setTook(response.took());

        if (response.errors()) {
            List<String> errors = new ArrayList<>();
            int failedCount = 0;
            int successCount = 0;

            for (BulkResponseItem item : response.items()) {
                if (item.error() != null) {
                    String errorMsg = String.format("Document %s failed: %s", item.id(), item.error().reason());
                    errors.add(errorMsg);
                    log.error(errorMsg);
                    failedCount++;
                } else {
                    successCount++;
                }
            }

            result.setSuccess(false);
            result.setIndexedCount(successCount);
            result.setDeletedCount(toDelete.size() - failedCount);
            result.setErrors(errors);
            result.setFailedCount(failedCount);

            log.error("Bulk operation partially failed: {} success, {} failed", successCount, failedCount);

            throw new BulkIndexException(
                    String.format("Bulk index failed: %d errors", failedCount),
                    result
            );
        } else {
            log.info("Successfully indexed {} documents, deleted {} documents",
                    toCreateOrUpdate.size(), toDelete.size());

            result.setSuccess(true);
            result.setIndexedCount(toCreateOrUpdate.size());
            result.setDeletedCount(toDelete.size());
        }

        return result;
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

    public static class BulkIndexException extends RuntimeException {
        private final BulkIndexResult result;

        public BulkIndexException(String message, BulkIndexResult result) {
            super(message);
            this.result = result;
        }

        public BulkIndexResult getResult() {
            return result;
        }
    }

    @Getter
    public static class BulkIndexResult {
        private boolean success;
        private int indexedCount;
        private int deletedCount;
        private int failedCount;
        private long took;
        private List<String> errors;

        public void setSuccess(boolean success) { this.success = success; }
        public void setIndexedCount(int indexedCount) { this.indexedCount = indexedCount; }
        public void setDeletedCount(int deletedCount) { this.deletedCount = deletedCount; }
        public void setFailedCount(int failedCount) { this.failedCount = failedCount; }
        public void setTook(long took) { this.took = took; }
        public void setErrors(List<String> errors) { this.errors = errors; }
    }
}
