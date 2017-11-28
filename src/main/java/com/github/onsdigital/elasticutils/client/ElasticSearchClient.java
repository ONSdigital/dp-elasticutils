package com.github.onsdigital.elasticutils.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.onsdigital.elasticutils.client.bulk.configuration.BulkProcessorConfiguration;
import com.github.onsdigital.elasticutils.util.ElasticSearchHelper;
import com.github.onsdigital.elasticutils.util.JsonUtils;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy;

/**
 * @author sullid (David Sullivan) on 16/11/2017
 * @project dp-elasticutils
 *
 * Single class to expose index/search/delete APIs for both the HTTP and TCP clients.
 * The idea is to reduce code duplication/maintenance when choosing between the
 * HTTP or TCP transport layers.
 */
public abstract class ElasticSearchClient<T> implements DefaultSearchClient<T> {

    private final Logger LOGGER = LoggerFactory.getLogger(ElasticSearchClient.class);

    protected DocumentType documentType = DocumentType.DOCUMENT;

    protected final String indexName;
    protected final BulkProcessorConfiguration bulkProcessorConfiguration;
    protected final Class<T> returnClass;

    private static ObjectMapper MAPPER = new ObjectMapper();

    public ElasticSearchClient(final String indexName, final BulkProcessorConfiguration bulkProcessorConfiguration,
                               final Class<T> returnClass) {
        this.indexName = indexName;
        this.bulkProcessorConfiguration = bulkProcessorConfiguration;
        this.returnClass = returnClass;

        // Ensure the client is closed properly on shutdown
        Runtime.getRuntime().addShutdownHook(new ShutDownThread(this));
    }

    @Override
    public void finalize() throws Throwable {
        // Close the Client connection
        this.close();

        // Invoke super
        super.finalize();
    }

    // SEARCH //

    // This throws IOException due to the HTTP REST client
    public SearchHits search(QueryBuilder qb) throws IOException {
        return search(qb, SearchType.DFS_QUERY_THEN_FETCH);
    }

    public abstract SearchHits search(QueryBuilder qb, SearchType searchType) throws IOException;

    public abstract SearchResponse search(SearchRequest searchRequest) throws IOException;

    public List<T> searchAndDeserialize(QueryBuilder qb) throws IOException {
        SearchHits searchHits = search(qb);
        return deserialize(searchHits);
    }

    public List<T> deserialize(SearchHits searchHits) {
        List<T> results = new ArrayList<>();

        searchHits.forEach(hit -> {
            try {
                results.add(MAPPER.readValue(hit.getSourceAsString(), returnClass));
            } catch (IOException e) {
                LOGGER.error("Error unmarshalling from json", e);
            }
        });

        return results;
    }

    // INDEX //

    protected abstract IndexResponse indexWithRefreshPolicy(IndexRequest indexRequest, RefreshPolicy refreshPolicy) throws IOException;

    public abstract boolean indexExists() throws IOException;

    public abstract boolean createIndex() throws IOException;

    public IndexResponse indexAndRefresh(T entity) throws IOException {
        Optional<byte[]> messageBytes = JsonUtils.convertJsonToBytes(entity);

        if (messageBytes.isPresent()) {
            IndexRequest indexRequest = this.createIndexRequest(messageBytes.get());

            return this.indexWithRefreshPolicy(indexRequest, WriteRequest.RefreshPolicy.WAIT_UNTIL);
        } else {
            throw new IOException(String.format("Failed to convert entity %s to byte array", entity));
        }
    }

    @Override
    public void index(T entity) {
        index(Arrays.asList(entity));
    }

    @Override
    public void index(List<T> entities) {
        index(entities.stream());
    }

    @Override
    public void index(Stream<T> entities) {
        BulkProcessor bulkProcessor = this.getBulkProcessor();
        entities
                .map(x -> JsonUtils.convertJsonToBytes(x))
                .filter(x -> x.isPresent())
                .map(x -> createIndexRequest(x.get()))
                .forEach(bulkProcessor::add);
    }

    public void bulkIndexWithRefreshInterval(List<T> entities) throws IOException {
        bulkIndexWithRefreshInterval(entities.stream());
    }

    public void bulkIndexWithRefreshInterval(Stream<T> entities) throws IOException {
        if (!this.indexExists()) {
            LOGGER.info("Creating index: {}", this.indexName);
            this.createIndex();
        }
        this.updateIndexRefreshInterval(30);
        this.index(entities);
        this.updateIndexRefreshInterval(1);
    }

    public IndexRequest createIndexRequest(byte[] messageBytes) {
        return createIndexRequest(messageBytes, XContentType.JSON);
    }

    public abstract IndexRequest createIndexRequest(byte[] messageBytes, XContentType xContentType);

    public IndexRequest createIndexRequestWithPipeline(byte[] messageBytes, String pipeline) {
        return createIndexRequestWithPipeline(messageBytes, pipeline, XContentType.JSON);
    }

    public abstract IndexRequest createIndexRequestWithPipeline(byte[] messageBytes, String pipeline, XContentType xContentType);

    public boolean resetRefreshInterval() throws IOException {
        return updateIndexRefreshInterval(1);
    }

    public boolean updateIndexRefreshInterval(int interval) throws IOException {
        Settings settings = Settings.builder().put("refresh_interval", interval + "s").build();
        return this.updateIndexSettings(settings);
    }

    public abstract boolean updateIndexSettings(Settings settings) throws IOException;

    protected abstract BulkProcessor getBulkProcessor();

    // DELETE //

    public abstract DeleteResponse deleteById(String id) throws IOException;

    // CLOSE //

    @Override
    public void flush() {
        this.getBulkProcessor().flush();
    }

    @Override
    public synchronized boolean awaitClose(long timeout, TimeUnit unit) throws InterruptedException {
        return this.getBulkProcessor().awaitClose(timeout, unit);
    }

    public abstract void close() throws Exception;

    // MISC //

    public abstract ElasticSearchHelper.ClientType getClientType();

    public enum DocumentType {
        DOCUMENT("document");

        private String documentType;

        DocumentType(String documentType) {
            this.documentType = documentType;
        }

        public String getDocumentType() {
            return documentType;
        }
    }

    private static class ShutDownThread extends Thread {

        private final Logger LOGGER = LoggerFactory.getLogger(ShutDownThread.class);

        private ElasticSearchClient client;

        public ShutDownThread(ElasticSearchClient client) {
            this.client = client;
        }

        @Override
        public void run() {
            try {
                if (LOGGER.isDebugEnabled()) LOGGER.debug("Shutting down ElasticSearchClient");
                client.close();
                if (LOGGER.isDebugEnabled()) LOGGER.debug("Successfully shut down ElasticSearchClient");
            } catch (Exception e) {
                LOGGER.error("Unable to close ElasticSearchClient", e);
            }
        }
    }

}
