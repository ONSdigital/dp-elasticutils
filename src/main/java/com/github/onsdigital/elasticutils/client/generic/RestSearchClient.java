package com.github.onsdigital.elasticutils.client.generic;

import com.github.onsdigital.elasticutils.client.bulk.configuration.BulkProcessorConfiguration;
import com.github.onsdigital.elasticutils.client.http.SimpleRestClient;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.main.MainResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.common.http.HttpMethod;

import java.io.IOException;
import java.util.StringJoiner;

/**
 * @author sullid (David Sullivan) on 29/11/2017
 * @project dp-elasticutils
 */
public class RestSearchClient<T> extends ElasticSearchClient<T> {

    private SimpleRestClient client;
    private final BulkProcessorConfiguration configuration;

    public RestSearchClient(SimpleRestClient client, String index, final BulkProcessorConfiguration configuration,
                            final Class<T> returnClass) {
        super(index, returnClass);
        this.client = client;
        this.configuration = configuration;
    }

    // INDEX //

    @Override
    public IndexRequest createIndexRequestWithPipeline(byte[] messageBytes, String pipeline, XContentType xContentType) {
        IndexRequest indexRequest = new IndexRequest(super.index)
                .source(messageBytes, XContentType.JSON)
                .setPipeline(pipeline)
                .type(super.type.getType());

        return indexRequest;
    }

    @Override
    protected BulkProcessor getBulkProcessor() {
        return this.configuration.build(this.client);
    }

    @Override
    public IndexResponse index(IndexRequest request) throws IOException {
        IndexResponse response = this.client.index(request);
        return response;
    }

    // SEARCH //

    @Override
    public ElasticSearchResponse<T> search(SearchRequest request) throws IOException {
        SearchResponse response = this.client.search(request);
        ElasticSearchResponse<T> elasticSearchResponse = new ElasticSearchResponse<>(response, super.returnClass);
        return elasticSearchResponse;
    }

    // DELETE //

    public Response dropIndex() throws IOException {
        RestClient client = this.getLowLevelClient();

        String endpoint = endpoint(super.index);
        Response response = client.performRequest(HttpMethod.DELETE.method(), endpoint);
        return response;
    }

    // MISC //

    public MainResponse info() throws IOException {
        return this.client.info();
    }

    // LOW LEVEL CLIENT //

    protected RestClient getLowLevelClient() {
        return this.client.getLowLevelClient();
    }

    static String endpoint(String[] indices, String[] types, String endpoint) {
        return endpoint(String.join(",", indices), String.join(",", types), endpoint);
    }

    /**
     * Utility method to build request's endpoint.
     */
    static String endpoint(String... parts) {
        StringJoiner joiner = new StringJoiner("/", "/", "");
        for (String part : parts) {
            if (Strings.hasLength(part)) {
                joiner.add(part);
            }
        }
        return joiner.toString();
    }
}
