package com.github.onsdigital.elasticutils.client;

import com.github.onsdigital.elasticutils.client.bulk.configuration.BulkProcessorConfiguration;
import com.github.onsdigital.elasticutils.indicies.ElasticIndexNames;
import com.github.onsdigital.elasticutils.util.ElasticSearchHelper;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.main.MainResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
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
    protected IndexResponse executeIndexAndRefresh(IndexRequest indexRequest) throws IOException {
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

    public Response deleteIndex(ElasticIndexNames indexName) throws IOException {
        /*
        Uses the low-level API to make a DELETE request against an entire index.
         */
        String endPoint = getIndexEndPoint(indexName);

        Response response = this.getLowLevelClient().performRequest(
                HttpRequestType.DELETE.getRequestType(), endPoint
        );

        return response;
    }

    public static String getIndexEndPoint(ElasticIndexNames indexName) {
        return "/" + indexName.getIndexName();
    }

    // ADMIN //

    public MainResponse info() throws IOException {
        MainResponse response = this.client.info();
        return response;
    }

    protected BulkProcessor getBulkProcessor() {
        return this.bulkProcessor;
    }

    public RestHighLevelClient getClient() {
        return this.client;
    }

    public RestClient getLowLevelClient() {
        return this.client.getLowLevelClient();
    }

    @Override
    public ElasticSearchHelper.ClientType getClientType() {
        return ElasticSearchHelper.ClientType.REST;
    }

    public enum HttpRequestType {
        GET("GET"),
        POST("POST"),
        PUT("PUT"),
        DELETE("DELETE");

        private String requestType;

        HttpRequestType(String requestType) {
            this.requestType = requestType;
        }

        public String getRequestType() {
            return requestType;
        }
    }
}
