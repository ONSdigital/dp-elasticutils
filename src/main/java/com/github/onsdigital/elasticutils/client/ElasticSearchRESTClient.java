package com.github.onsdigital.elasticutils.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.onsdigital.elasticutils.client.bulk.configuration.BulkProcessorConfiguration;
import com.github.onsdigital.elasticutils.util.ElasticSearchHelper;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.main.MainResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy;

/**
 * @author sullid (David Sullivan) on 16/11/2017
 * @project dp-elasticutils
 *
 * Implementation of ElasticSearch REST client
 */
public class ElasticSearchRESTClient<T> extends ElasticSearchClient<T> {

    private RestHighLevelClient client;
    private BulkProcessor bulkProcessor;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public ElasticSearchRESTClient(String hostName, String indexName, Class<T> returnClass) {
        this(hostName, ElasticSearchHelper.DEFAULT_HTTP_PORT, indexName,
                ElasticSearchHelper.getDefaultBulkProcessorConfiguration(), returnClass);
    }

    public ElasticSearchRESTClient(String hostName, int http_port, String indexName, Class<T> returnClass) {
        this(hostName, http_port, indexName,
                ElasticSearchHelper.getDefaultBulkProcessorConfiguration(), returnClass);
    }

    public ElasticSearchRESTClient(String hostName, int http_port, String indexName,
                                   final BulkProcessorConfiguration bulkProcessorConfiguration, Class<T> returnClass) {
        super(hostName, http_port, indexName, bulkProcessorConfiguration, returnClass);
        this.client = ElasticSearchHelper.getRestClient(super.hostName, super.port);
        this.bulkProcessor = super.bulkProcessorConfiguration.build(this.client);
    }

    // SEARCH //

    @Override
    public SearchResponse search(SearchRequest searchRequest) throws IOException {
        return this.client.search(searchRequest);
    }

    @Override
    public SearchHits search(QueryBuilder qb, SearchType searchType) throws IOException {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
                .query(qb);

        SearchRequest searchRequest = new SearchRequest(this.indexName)
                .source(sourceBuilder)
                .searchType(searchType);

        SearchResponse searchResponse = this.client.search(searchRequest);

        return searchResponse.getHits();
    }

    // INDEX //

    @Override
    protected IndexResponse indexWithRefreshPolicy(IndexRequest indexRequest, RefreshPolicy refreshPolicy) throws IOException {
        indexRequest.setRefreshPolicy(refreshPolicy);

        IndexResponse indexResponse = this.client.index(indexRequest);
        return indexResponse;
    }

    @Override
    public boolean indexExists(String indexName) throws IOException {
        try (RestClient lowLevelClient = this.getLowLevelClient()) {
            Response response = lowLevelClient.performRequest(
                    HttpRequestType.HEAD.getRequestType(), indexName
            );

            return response.getStatusLine().getStatusCode() == HttpStatus.SC_OK;
        }
    }

    @Override
    public IndexRequest createIndexRequest(byte[] messageBytes) {
        return this.createIndexRequest(messageBytes, XContentType.JSON);
    }

    @Override
    public IndexRequest createIndexRequest(byte[] messageBytes, XContentType xContentType) {
        IndexRequest indexRequest = new IndexRequest(super.indexName)
                .source(messageBytes, XContentType.JSON)
                .type(super.documentType.getDocumentType());

        return indexRequest;
    }

    public IndexRequest createIndexRequest(String id, byte[] messageBytes, XContentType xContentType) {
        IndexRequest indexRequest = new IndexRequest(super.indexName)
                .source(messageBytes, XContentType.JSON)
                .id(id)
                .type(super.documentType.getDocumentType());

        return indexRequest;
    }

    @Override
    public boolean updateIndexSettings(Settings settings) throws IOException {
        Map<String, String> settingsMap = settings.getAsMap();

        try (RestClient lowLevelClient = this.getLowLevelClient()) {

            String json = MAPPER.writeValueAsString(settingsMap);
            HttpEntity entity = new NStringEntity(json, ContentType.APPLICATION_JSON);

            String api = getIndexEndPoint(super.indexName) + "/_settings";
            Response response = lowLevelClient.performRequest(HttpRequestType.PUT.getRequestType(), api, Collections.<String, String>emptyMap(),
                    entity);

            return (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK);
        }
    }

    // DELETE //

    @Override
    public DeleteResponse deleteById(String id) throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest()
                .index(super.indexName)
                .type(super.documentType.getDocumentType())
                .id(id);

        DeleteResponse deleteResponse = this.client.delete(deleteRequest);
        return deleteResponse;
    }

    public Response deleteIndex(String indexName) throws IOException {
        /*
        Uses the low-level API to make a DELETE request against an entire index.
         */
        String endPoint = getIndexEndPoint(indexName);

        try (RestClient lowLevelClient = this.getLowLevelClient()) {
            Response response = lowLevelClient.performRequest(
                    HttpRequestType.DELETE.getRequestType(), endPoint
            );

            return response;
        }
    }

    public static String getIndexEndPoint(String indexName) {
        return "/" + indexName;
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

    // Use the low-level client with care, and ALWAYS within a try with resource block (see above)
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
        DELETE("DELETE"),
        HEAD("HEAD");

        private String requestType;

        HttpRequestType(String requestType) {
            this.requestType = requestType;
        }

        public String getRequestType() {
            return requestType;
        }
    }
}
