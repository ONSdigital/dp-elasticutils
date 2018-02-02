package com.github.onsdigital.elasticutils.client.generic;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.github.onsdigital.elasticutils.action.delete.SimpleDeleteRequestBuilder;
import com.github.onsdigital.elasticutils.action.index.SimpleIndexRequestBuilder;
import com.github.onsdigital.elasticutils.action.search.SimpleSearchRequestBuilder;
import com.github.onsdigital.elasticutils.client.DefaultSearchClient;
import com.github.onsdigital.elasticutils.client.pipeline.Pipeline;
import com.github.onsdigital.elasticutils.client.type.DocumentType;
import com.github.onsdigital.elasticutils.util.JsonUtils;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * @author sullid (David Sullivan) on 29/11/2017
 * @project dp-elasticutils
 */
public abstract class ElasticSearchClient<T> implements DefaultSearchClient<T> {

    protected static final Logger LOGGER = LoggerFactory.getLogger(ElasticSearchClient.class);

    // INDEX //

    @Override
    public void bulk(String index, DocumentType documentType, T entity) {
        bulk(index, documentType, Arrays.asList(entity));
    }

    @Override
    public void bulk(String index, DocumentType documentType, List<T> entities) {
        bulk(index, documentType, entities.stream());
    }

    @Override
    public void bulk(String index, DocumentType documentType, Stream<T> entities) {
        bulk(index, documentType, entities, XContentType.JSON);
    }

    public void bulk(String index, DocumentType documentType, Stream<T> entities, XContentType contentType) {
        this.bulk(index, documentType, entities, contentType, JsonInclude.Include.USE_DEFAULTS);
    }

    public void bulk(String index, DocumentType documentType, Stream<T> entities,
                     XContentType contentType, JsonInclude.Include include) {
        BulkProcessor bulkProcessor = this.getBulkProcessor();
        entities
                .map(x -> JsonUtils.convertJsonToBytes(x, include))
                .filter(x -> x.isPresent())
                .map(x -> createIndexRequest(index, documentType, x.get(), contentType))
                .forEach(bulkProcessor::add);
    }

    public void addToBulk(IndexRequest indexRequest) {
        this.addToBulk(Arrays.asList(indexRequest));
    }

    public void addToBulk(List<IndexRequest> indexRequests) {
        this.addToBulk(indexRequests.stream());
    }

    public void addToBulk(Stream<IndexRequest> indexRequests) {
        BulkProcessor bulkProcessor = this.getBulkProcessor();
        indexRequests.forEach(bulkProcessor::add);
    }

    protected IndexRequest createIndexRequest(String index, DocumentType documentType, byte[] messageBytes, XContentType xContentType) {
        return createIndexRequestWithPipeline(index, documentType, null, messageBytes, xContentType);
    }

    protected IndexRequest createIndexRequestWithPipeline(String index, DocumentType documentType, Pipeline pipeline, byte[] messageBytes, XContentType xContentType) {
        SimpleIndexRequestBuilder indexRequestBuilder = this.prepareIndex()
                .setIndex(index)
                .setType(documentType.getType())
                .setSource(messageBytes, xContentType);

        if (pipeline != null) {
            indexRequestBuilder.setPipeline(pipeline.getPipeline());
        }

        IndexRequest indexRequest = indexRequestBuilder.request();
        return indexRequest;
    }

    public abstract BulkProcessor getBulkProcessor();

    // SEARCH //

    public abstract SearchResponse search(SearchRequest request) throws IOException;

    public abstract SearchResponse searchScroll(SearchScrollRequest request) throws IOException;

    // DELETE //

    public abstract boolean dropIndex(String index) throws IOException;

    // ADMIN //

    public abstract boolean indexExists(String index);

    public abstract boolean createIndex(String index, DocumentType documentType, Settings settings, Map<String, Object> mapping);

    // BUILDERS //

    @Override
    public SimpleIndexRequestBuilder prepareIndex() {
        return new SimpleIndexRequestBuilder();
    }

    @Override
    public SimpleSearchRequestBuilder prepareSearch(String[] indices) {
        return new SimpleSearchRequestBuilder(indices);
    }

    @Override
    public SimpleDeleteRequestBuilder prepareDelete() {
        return new SimpleDeleteRequestBuilder();
    }

    // SHUTDOWN //

    @Override
    public void flush() {
        this.getBulkProcessor().flush();
    }

    @Override
    public synchronized boolean awaitClose(long timeout, TimeUnit unit) throws InterruptedException {
        return this.getBulkProcessor().awaitClose(timeout, unit);
    }

    @Override
    public void close() throws Exception {
        LOGGER.info("Closing client connection");
        this.shutdown();
        LOGGER.info("Successfully closed client connection");
    }

    public abstract void shutdown() throws IOException;

    private static class ShutDownThread extends Thread {

        private final Logger LOGGER = LoggerFactory.getLogger(ShutDownThread.class);

        private ElasticSearchClient client;

        public ShutDownThread(ElasticSearchClient client) {
            this.client = client;
        }

        @Override
        public void run() {
            try {
                LOGGER.info("Shutting down ElasticSearchClient");
                client.shutdown();
                LOGGER.info("Successfully shut down ElasticSearchClient");
            } catch (Exception e) {
                LOGGER.error("Unable to close ElasticSearchClient", e);
            }
        }
    }
}
