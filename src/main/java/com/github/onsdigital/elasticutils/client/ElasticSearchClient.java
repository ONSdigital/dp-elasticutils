package com.github.onsdigital.elasticutils.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.onsdigital.elasticutils.client.bulk.configuration.BulkProcessorConfiguration;
import com.github.onsdigital.elasticutils.indicies.ElasticIndexNames;
import com.github.onsdigital.elasticutils.util.ElasticSearchHelper;
import com.github.onsdigital.elasticutils.util.JsonUtils;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHits;

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

    protected DocumentType documentType = DocumentType.DOCUMENT;

    protected final String hostName;
    protected final int port;
    protected final ElasticIndexNames indexName;
    protected final BulkProcessorConfiguration bulkProcessorConfiguration;
    protected Class<T> returnClass;

    private static ObjectMapper MAPPER = new ObjectMapper();

    public ElasticSearchClient(String hostName, int port, ElasticIndexNames indexName,
                               BulkProcessorConfiguration bulkProcessorConfiguration,
                               Class<T> returnClass) {
        this.hostName = hostName;
        this.port = port;
        this.indexName = indexName;
        this.bulkProcessorConfiguration = bulkProcessorConfiguration;
        this.returnClass = returnClass;
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
    public abstract SearchHits search(QueryBuilder qb) throws IOException;

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
                e.printStackTrace();
            }
        });

        return results;
    }

    // INDEX //

    protected abstract IndexResponse indexWithRefreshPolicy(IndexRequest indexRequest, RefreshPolicy refreshPolicy) throws IOException;

    public abstract boolean indexExists(ElasticIndexNames indexName) throws IOException;

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

    // DELETE //

    public abstract DeleteResponse deleteById(String id) throws IOException;

    @Override
    public void flush() {
        this.getBulkProcessor().flush();
    }

    @Override
    public synchronized boolean awaitClose(long timeout, TimeUnit unit) throws InterruptedException {
        return this.getBulkProcessor().awaitClose(timeout, unit);
    }

    public void close() throws Exception {
        this.getBulkProcessor().close();
    }

    public abstract IndexRequest createIndexRequest(byte[] messageBytes);

    public abstract IndexRequest createIndexRequest(byte[] messageBytes, XContentType xContentType);

    public abstract ElasticSearchHelper.ClientType getClientType();

    protected abstract BulkProcessor getBulkProcessor();

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

}
