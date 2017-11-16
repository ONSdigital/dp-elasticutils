package com.github.onsdigital.elasticutils.client;

import com.github.onsdigital.elasticutils.client.bulk.configuration.BulkProcessorConfiguration;
import com.github.onsdigital.elasticutils.indicies.ElasticIndexNames;
import com.github.onsdigital.elasticutils.util.ElasticSearchHelper;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;

/**
 * @author sullid (David Sullivan) on 16/11/2017
 * @project dp-elasticutils
 *
 * Implementation of ElasticSearch REST client
 */
public class ElasticSearchRESTClient<T> extends ElasticSearchClient<T> {

    private RestHighLevelClient client;
    private BulkProcessor bulkProcessor;

    public ElasticSearchRESTClient(String hostName, ElasticIndexNames indexName, Class<T> returnClass) {
        this(hostName, ElasticSearchHelper.DEFAULT_HTTP_PORT, indexName,
                ElasticSearchHelper.getDefaultBulkProcessorConfiguration(), returnClass);
    }

    public ElasticSearchRESTClient(String hostName, int http_port, ElasticIndexNames indexName, Class<T> returnClass) {
        this(hostName, http_port, indexName,
                ElasticSearchHelper.getDefaultBulkProcessorConfiguration(), returnClass);
    }

    public ElasticSearchRESTClient(String hostName, int http_port, ElasticIndexNames indexName,
                                   final BulkProcessorConfiguration bulkProcessorConfiguration, Class<T> returnClass) {
        super(hostName, http_port, indexName, bulkProcessorConfiguration, returnClass);
        this.client = ElasticSearchHelper.getRestClient(super.hostName, super.port);
        this.bulkProcessor = super.bulkProcessorConfiguration.build(this.client);
    }

    // SEARCH //

    @Override
    public SearchHits search(QueryBuilder qb) throws IOException {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
                .query(qb);

        SearchRequest searchRequest = new SearchRequest(this.indexName.getIndexName()).source(sourceBuilder);
        SearchResponse searchResponse = this.client.search(searchRequest);

        return searchResponse.getHits();
    }

    // INDEX //

    @Override
    protected IndexResponse performSyncIndex(IndexRequest indexRequest) throws IOException {
        IndexResponse indexResponse = this.client.index(indexRequest);
        return indexResponse;
    }

    @Override
    public IndexRequest createIndexRequest(byte[] messageBytes) {
        return this.createIndexRequest(messageBytes, XContentType.JSON);
    }

    @Override
    public IndexRequest createIndexRequest(byte[] messageBytes, XContentType xContentType) {
        IndexRequest indexRequest = new IndexRequest(super.indexName.getIndexName())
                .source(messageBytes, XContentType.JSON)
                .type(super.indexType.getIndexType());

        return indexRequest;
    }

    public IndexRequest createIndexRequest(String id, byte[] messageBytes, XContentType xContentType) {
        IndexRequest indexRequest = new IndexRequest(super.indexName.getIndexName())
                .source(messageBytes, XContentType.JSON)
                .id(id)
                .type(super.indexType.getIndexType());

        return indexRequest;
    }

    // DELETE //

    @Override
    public DeleteResponse deleteById(String id) throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest()
                .index(super.indexName.getIndexName())
                .type(super.indexType.getIndexType())
                .id(id);

        System.out.println(deleteRequest.toString());

        DeleteResponse deleteResponse = this.client.delete(deleteRequest);
        return deleteResponse;
    }

    protected BulkProcessor getBulkProcessor() {
        return this.bulkProcessor;
    }

    @Override
    public ElasticSearchHelper.ClientType getClientType() {
        return ElasticSearchHelper.ClientType.REST;
    }
}
