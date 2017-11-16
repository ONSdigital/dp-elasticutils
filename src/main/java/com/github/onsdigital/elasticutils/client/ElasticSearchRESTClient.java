package com.github.onsdigital.elasticutils.client;

import com.github.onsdigital.elasticutils.client.bulk.configuration.BulkProcessorConfiguration;
import com.github.onsdigital.elasticutils.indicies.ElasticIndexNames;
import com.github.onsdigital.elasticutils.util.ElasticSearchHelper;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

/**
 * @author sullid (David Sullivan) on 16/11/2017
 * @project dp-elasticutils
 */
public class ElasticSearchRESTClient extends ElasticSearchClient {

    private RestHighLevelClient client;
    private BulkProcessor bulkProcessor;

    public ElasticSearchRESTClient(String hostName) {
        this(hostName, ElasticSearchHelper.DEFAULT_HTTP_PORT, ElasticSearchHelper.getDefaultBulkProcessorConfiguration());
    }

    public ElasticSearchRESTClient(String hostName, int http_port) {
        this(hostName, http_port, ElasticSearchHelper.getDefaultBulkProcessorConfiguration());
    }

    public ElasticSearchRESTClient(String hostName, int http_port, final BulkProcessorConfiguration bulkProcessorConfiguration) {
        super(hostName, http_port, bulkProcessorConfiguration);
        this.client = ElasticSearchHelper.getRestClient(super.hostName, super.port);
        this.bulkProcessor = super.bulkProcessorConfiguration.build(this.client);
    }

    @Override
    protected IndexRequest createIndexRequest(ElasticIndexNames indexName, byte[] messageBytes) {
        return this.createIndexRequest(indexName, messageBytes, XContentType.JSON);
    }

    @Override
    protected IndexRequest createIndexRequest(ElasticIndexNames indexName, byte[] messageBytes, XContentType xContentType) {
        IndexRequest indexRequest = new IndexRequest(indexName.getIndexName())
                .source(messageBytes, XContentType.JSON)
                .type(super.indexType.getIndexType());

        return indexRequest;
    }

    protected BulkProcessor getBulkProcessor() {
        return this.bulkProcessor;
    }

    @Override
    public ClientType getClientType() {
        return ClientType.REST;
    }
}
