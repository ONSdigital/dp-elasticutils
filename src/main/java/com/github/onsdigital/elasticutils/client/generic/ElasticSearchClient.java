package com.github.onsdigital.elasticutils.client.generic;

import com.github.onsdigital.elasticutils.action.delete.SimpleDeleteRequestBuilder;
import com.github.onsdigital.elasticutils.action.index.SimpleIndexRequestBuilder;
import com.github.onsdigital.elasticutils.action.search.SimpleSearchRequestBuilder;
import com.github.onsdigital.elasticutils.client.DefaultSearchClient;
import com.github.onsdigital.elasticutils.client.type.DocumentType;
import com.github.onsdigital.elasticutils.util.JsonUtils;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * @author sullid (David Sullivan) on 29/11/2017
 * @project dp-elasticutils
 */
public abstract class ElasticSearchClient<T> implements DefaultSearchClient<T> {

    protected static final Logger LOGGER = LoggerFactory.getLogger(ElasticSearchClient.class);

    protected final String index;
    protected final DocumentType type;
    protected final Class<T> returnClass;

    public ElasticSearchClient(String index, final Class<T> returnClass) {
        this.index = index;
        this.type = DocumentType.DOCUMENT;
        this.returnClass = returnClass;
//        Runtime.getRuntime().addShutdownHook(new ElasticSearchClient.ShutDownThread(this));
    }

    // INDEX //

    @Override
    public void bulk(T entity) {
        bulk(Arrays.asList(entity));
    }

    @Override
    public void bulk(List<T> entities) {
        bulk(entities.stream());
    }

    @Override
    public void bulk(Stream<T> entities) {
        bulk(entities, XContentType.JSON);
    }

    public void bulk(Stream<T> entities, XContentType contentType) {
        BulkProcessor bulkProcessor = this.getBulkProcessor();
        entities
                .map(x -> JsonUtils.convertJsonToBytes(x))
                .filter(x -> x.isPresent())
                .map(x -> createIndexRequest(x.get(), contentType))
                .forEach(bulkProcessor::add);
    }

    protected IndexRequest createIndexRequest(byte[] messageBytes, XContentType xContentType) {
        return createIndexRequestWithPipeline(messageBytes, null, xContentType);
    }

    protected abstract IndexRequest createIndexRequestWithPipeline(byte[] messageBytes, String pipeline, XContentType xContentType);

    protected abstract BulkProcessor getBulkProcessor();

    // SEARCH //

    public abstract ElasticSearchResponse<T> search(SearchRequest request) throws IOException;

    // BUILDERS //

    @Override
    public SimpleIndexRequestBuilder prepareIndex() {
        return new SimpleIndexRequestBuilder();
    }

    @Override
    public SimpleSearchRequestBuilder prepareSearch() {
        return new SimpleSearchRequestBuilder(this.index);
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
