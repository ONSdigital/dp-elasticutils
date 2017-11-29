package com.github.onsdigital.elasticutils.client.generic;

import com.github.onsdigital.elasticutils.client.bulk.configuration.BulkProcessorConfiguration;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;

/**
 * @author sullid (David Sullivan) on 29/11/2017
 * @project dp-elasticutils
 */
public class TransportSearchClient<T> extends ElasticSearchClient<T> {

    private TransportClient client;
    private final BulkProcessor bulkProcessor;

    public TransportSearchClient(TransportClient client, String index, final BulkProcessorConfiguration configuration,
                            final Class<T> returnClass) {
        super(index, returnClass);
        this.client = client;
        this.bulkProcessor = configuration.build(this.client);
    }

    // INDEX //

    @Override
    protected IndexRequest createIndexRequestWithPipeline(byte[] messageBytes, String pipeline, XContentType xContentType) {
        return this.client.prepareIndex()
                .setIndex(super.index)
                .setType(super.type.getType())
                .setSource(messageBytes, xContentType)
                .setPipeline(pipeline)
                .request();
    }

    @Override
    protected BulkProcessor getBulkProcessor() {
        return this.bulkProcessor;
    }

    @Override
    public IndexResponse index(IndexRequest request) throws IOException {
        IndexResponse response = this.client.index(request).actionGet();
        return response;
    }

    // SEARCH //

    @Override
    public ElasticSearchResponse<T> search(SearchRequest request) {
        SearchResponse response = this.client.search(request).actionGet();
        ElasticSearchResponse<T> elasticSearchResponse = new ElasticSearchResponse<>(response, super.returnClass);
        return elasticSearchResponse;
    }

    // DELETE //

    public DeleteIndexResponse dropIndex() {
        DeleteIndexRequest request = new DeleteIndexRequest()
                .indices(super.index);
        DeleteIndexResponse response = this.admin().indices().delete(request).actionGet();
        return response;
    }

    // ADMIN //

    public AdminClient admin() {
        return this.client.admin();
    }

    @Override
    public void shutdown() {
        this.client.close();
    }
}
