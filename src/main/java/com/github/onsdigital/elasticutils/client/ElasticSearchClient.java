package com.github.onsdigital.elasticutils.client;

import com.github.onsdigital.elasticutils.client.bulk.configuration.BulkProcessorConfiguration;
import com.github.onsdigital.elasticutils.indicies.ElasticIndexNames;
import com.github.onsdigital.elasticutils.util.JsonUtils;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * @author sullid (David Sullivan) on 16/11/2017
 * @project dp-elasticutils
 */
public abstract class ElasticSearchClient implements AutoCloseable {

    protected IndexType indexType = IndexType.DOCUMENT;

    protected final String hostName;
    protected final int port;
    protected final BulkProcessorConfiguration bulkProcessorConfiguration;

    public ElasticSearchClient(String hostName, int port, BulkProcessorConfiguration bulkProcessorConfiguration) {
        this.hostName = hostName;
        this.port = port;
        this.bulkProcessorConfiguration = bulkProcessorConfiguration;
    }

    public void index(ElasticIndexNames indexName, Object object) {
        index(indexName, Arrays.asList(object));
    }

    public void index(ElasticIndexNames indexName, List<Object> objectList) {
        index(indexName, objectList.stream());
    }

    public void index(ElasticIndexNames indexName, Stream<Object> objectStream) {
        BulkProcessor bulkProcessor = this.getBulkProcessor();
        objectStream
                .map(x -> JsonUtils.convertJsonToBytes(x))
                .filter(x -> x.isPresent())
                .map(x -> createIndexRequest(indexName, x.get()))
                .forEach(bulkProcessor::add);
    }

    public void flush() {
        this.getBulkProcessor().flush();
    }

    public synchronized boolean awaitClose(long timeout, TimeUnit unit) throws InterruptedException {
        return this.getBulkProcessor().awaitClose(timeout, unit);
    }

    public void close() throws Exception {
        this.getBulkProcessor().close();
    }

    protected abstract IndexRequest createIndexRequest(ElasticIndexNames indexName, byte[] messageBytes);

    protected abstract IndexRequest createIndexRequest(ElasticIndexNames indexName, byte[] messageBytes, XContentType xContentType);

    public abstract ClientType getClientType();

    public String getHostName() {
        return hostName;
    }

    public int getPort() {
        return port;
    }

    protected abstract BulkProcessor getBulkProcessor();

    public enum IndexType {
        DOCUMENT("document");

        private String indexType;

        IndexType(String indexType) {
            this.indexType = indexType;
        }

        public String getIndexType() {
            return indexType;
        }
    }

    public enum ClientType {
        TCP("TCP"),
        REST("REST");

        private String clientType;

        ClientType(String clientType) {
            this.clientType = clientType;
        }

        public String getClientType() {
            return clientType;
        }
    }

}
