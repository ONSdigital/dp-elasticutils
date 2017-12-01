package com.github.onsdigital.elasticutils.client.generic;

import com.github.onsdigital.elasticutils.client.Host;
import com.github.onsdigital.elasticutils.client.bulk.configuration.BulkProcessorConfiguration;
import com.github.onsdigital.elasticutils.util.ElasticSearchHelper;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Map;

/**
 * @author sullid (David Sullivan) on 29/11/2017
 * @project dp-elasticutils
 */
public class TransportSearchClient<T> extends ElasticSearchClient<T> {

    private TransportClient client;
    private final BulkProcessor bulkProcessor;

    public TransportSearchClient(TransportClient client, final BulkProcessorConfiguration configuration) {
        this.client = client;
        this.bulkProcessor = configuration.build(this.client);
    }

    // INDEX //

    @Override
    protected IndexRequest createIndexRequestWithPipeline(String index, byte[] messageBytes, String pipeline, XContentType xContentType) {
        return this.client.prepareIndex()
                .setIndex(index)
                .setType(super.DEFAULT_DOCUMENT_TYPE.getType())
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
    public SearchResponse search(SearchRequest request) {
        SearchResponse response = this.client.search(request).actionGet();
        return response;
    }

    // DELETE //

    @Override
    public boolean dropIndex(String index) {
        DeleteIndexRequest request = new DeleteIndexRequest()
                .indices(index);
        DeleteIndexResponse response = this.admin().indices().delete(request).actionGet();
        return response.isAcknowledged();
    }

    // ADMIN //

    @Override
    public boolean indexExists(String index) {
        IndicesExistsResponse response = this.admin().indices().prepareExists(index)
                .execute().actionGet();
        return response.isExists();
    }

    @Override
    public boolean createIndex(String index, Settings settings, Map<String, Object> mapping) {
        CreateIndexRequest request = new CreateIndexRequest()
                .index(index)
                .settings(settings)
                .mapping(super.DEFAULT_DOCUMENT_TYPE.getType(), mapping);
        CreateIndexResponse response = this.admin().indices().create(request).actionGet();
        return response.isAcknowledged();
    }

    public AdminClient admin() {
        return this.client.admin();
    }

    public static TransportSearchClient getLocalClient() throws UnknownHostException {
        BulkProcessorConfiguration configuration = ElasticSearchHelper.getDefaultBulkProcessorConfiguration();
        return getLocalClient(configuration);
    }

    public static TransportSearchClient getLocalClient(BulkProcessorConfiguration configuration) throws UnknownHostException {
        return new TransportSearchClient<>(ElasticSearchHelper.getTransportClient(Host.LOCALHOST), configuration);
    }

    @Override
    public void shutdown() {
        this.client.close();
    }
}
