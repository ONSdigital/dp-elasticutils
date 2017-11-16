package com.github.onsdigital.elasticutils.client;

import com.github.onsdigital.elasticutils.client.bulk.configuration.BulkProcessorConfiguration;
import com.github.onsdigital.elasticutils.indicies.ElasticIndexNames;
import com.github.onsdigital.elasticutils.util.ElasticSearchHelper;
import com.github.onsdigital.elasticutils.util.JsonUtils;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;

import java.net.UnknownHostException;
import java.util.stream.Stream;

/**
 * @author sullid (David Sullivan) on 16/11/2017
 * @project dp-elasticutils
 */
public class ElasticSearchTransportClient extends ElasticSearchClient {

    private final Client client;
    private final BulkProcessor bulkProcessor;

    public ElasticSearchTransportClient(String hostName) throws UnknownHostException {
        this(hostName, ElasticSearchHelper.DEFAULT_TCP_PORT);
    }

    public ElasticSearchTransportClient(String hostName, int transport_port) throws UnknownHostException {
        this(hostName, transport_port, Settings.EMPTY);
    }

    public ElasticSearchTransportClient(String hostName, int transport_port, Settings settings) throws UnknownHostException {
        this(hostName, transport_port, settings, ElasticSearchHelper.getDefaultBulkProcessorConfiguration());

    }

    public ElasticSearchTransportClient(String hostName, int transport_port, Settings settings,
                                        BulkProcessorConfiguration bulkProcessorConfiguration) throws UnknownHostException {
        super(hostName, transport_port, bulkProcessorConfiguration);
        this.client = ElasticSearchHelper.getTransportClient(super.hostName, super.port, settings);
        this.bulkProcessor = super.bulkProcessorConfiguration.build(client);
    }

    @Override
    protected IndexRequest createIndexRequest(ElasticIndexNames indexName, byte[] messageBytes) {
        return this.createIndexRequest(indexName, messageBytes, XContentType.JSON);
    }

    @Override
    protected IndexRequest createIndexRequest(ElasticIndexNames indexName, byte[] messageBytes, XContentType xContentType) {
        return this.client.prepareIndex()
                .setIndex(indexName.getIndexName())
                .setType(this.indexType.getIndexType())
                .setSource(messageBytes, xContentType)
                .request();
    }

    @Override
    public ClientType getClientType() {
        return ClientType.TCP;
    }

    @Override
    protected BulkProcessor getBulkProcessor() {
        return this.bulkProcessor;
    }

    public Client getClient() {
        return this.client;
    }

    public AdminClient admin() {
        return this.getClient().admin();
    }
}
