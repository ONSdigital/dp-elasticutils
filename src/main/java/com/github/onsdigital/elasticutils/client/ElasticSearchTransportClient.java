package com.github.onsdigital.elasticutils.client;

import com.github.onsdigital.elasticutils.client.bulk.configuration.BulkProcessorConfiguration;
import com.github.onsdigital.elasticutils.util.ElasticSearchHelper;
import org.apache.http.HttpStatus;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.search.SearchHits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.UnknownHostException;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy;

/**
 * @author sullid (David Sullivan) on 16/11/2017
 * @project dp-elasticutils
 *
 * Implementation of Elasticsearch TCP client
 */
public class ElasticSearchTransportClient<T> extends ElasticSearchClient<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticSearchTransportClient.class);

    private final Client client;
    private final BulkProcessor bulkProcessor;
    private static final BulkProcessorConfiguration DEFAULT_CONFIGURATION;

    static {
        DEFAULT_CONFIGURATION = ElasticSearchHelper.getDefaultBulkProcessorConfiguration();
    }

    public ElasticSearchTransportClient(final String hostName, final String indexName, final Class<T> returnClass) throws UnknownHostException {
        this(hostName, ElasticSearchHelper.DEFAULT_TCP_PORT, indexName, returnClass);
    }

    public ElasticSearchTransportClient(final String hostName, final int tcp_port, final String indexName, final Class<T> returnClass) throws UnknownHostException {
        this(hostName, tcp_port, indexName, Settings.EMPTY, returnClass);
    }

    public ElasticSearchTransportClient(final String hostName, final int tcp_port, final String indexName,
                                        final Settings settings,
                                        final Class<T> returnClass) throws UnknownHostException {
        this(hostName, tcp_port, indexName, settings, DEFAULT_CONFIGURATION, returnClass);
    }

    public ElasticSearchTransportClient(final String hostName, final int tcp_port, final String indexName,
                                   final Settings settings,
                                   final BulkProcessorConfiguration bulkProcessorConfiguration,
                                   final Class<T> returnClass) throws UnknownHostException {
        this(indexName, ElasticSearchHelper.getTransportClient(hostName, tcp_port, settings), bulkProcessorConfiguration, returnClass);
    }

    public ElasticSearchTransportClient(final String indexName,
                                   final TransportClient client,
                                   final BulkProcessorConfiguration bulkProcessorConfiguration,
                                   final Class<T> returnClass) {
        super(indexName, bulkProcessorConfiguration, returnClass);
        this.client = client;
        this.bulkProcessor = super.bulkProcessorConfiguration.build(this.client);
    }

    // SEARCH //

    @Override
    public SearchResponse search(SearchRequest searchRequest) throws IOException {
        return this.client.search(searchRequest).actionGet();
    }

    @Override
    public SearchHits search(QueryBuilder qb, SearchType searchType) {
        SearchResponse searchResponse = this.client.prepareSearch()
                .setIndices(this.indexName)
                .setTypes(this.documentType.getDocumentType())
                .setSearchType(searchType)
                .setQuery(qb)
                .setExplain(true)
                .execute().actionGet();

        SearchHits searchHits = searchResponse.getHits();
        return searchHits;
    }

    // INDEX //

    @Override
    protected IndexResponse indexWithRefreshPolicy(IndexRequest indexRequest, RefreshPolicy refreshPolicy) {
        indexRequest.setRefreshPolicy(refreshPolicy);

        IndexResponse indexResponse = this.client.index(indexRequest).actionGet();
        return indexResponse;
    }

    @Override
    public boolean indexExists() {
        IndicesExistsRequest request = new IndicesExistsRequest(super.indexName);

        IndicesExistsResponse response = this.admin().indices().exists(request).actionGet();
        return response.isExists();
    }

    @Override
    public boolean createIndex() throws IOException {
        IndexRequest request = this.client.prepareIndex()
                .setIndex(super.indexName)
                .request();

        IndexResponse response = this.client.index(request).actionGet();
        return (response.status().getStatus() == HttpStatus.SC_OK);
    }

    @Override
    public IndexRequest createIndexRequest(byte[] messageBytes, XContentType xContentType) {
        return createIndexRequestWithPipeline(messageBytes, null, xContentType);
    }

    @Override
    public IndexRequest createIndexRequestWithPipeline(byte[] messageBytes, String pipeline, XContentType xContentType) {
        return this.client.prepareIndex()
                .setIndex(super.indexName)
                .setType(this.documentType.getDocumentType())
                .setSource(messageBytes, xContentType)
                .setPipeline(pipeline)
                .request();
    }

    @Override
    public boolean updateIndexSettings(Settings settings) {
        UpdateSettingsResponse response = this.admin().indices().prepareUpdateSettings(super.indexName)
                .setSettings(settings)
                .get();
        return response.isAcknowledged();
    }

    // DELETE //

    @Override
    public DeleteResponse deleteById(String id) {
        // Synchronous
        DeleteRequest deleteRequest = new DeleteRequest()
                .index(super.indexName)
                .type(super.documentType.getDocumentType())
                .id(id)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);

        DeleteResponse deleteResponse = this.client.delete(deleteRequest).actionGet();
        return deleteResponse;
    }

    @Override
    public void close() throws Exception {
        this.bulkProcessor.close();
    }

    public void deleteByQuery(QueryBuilder qb) {
        DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
                .filter(qb)
                .source(super.indexName)
                .execute(new ActionListener<BulkByScrollResponse>() {
                    @Override
                    public void onResponse(BulkByScrollResponse response) {
                        long deleted = response.getDeleted();
                        LOGGER.info(String.format("Deleted %d records.", deleted));
                    }
                    @Override
                    public void onFailure(Exception e) {
                        LOGGER.error("Exception in deleteByQuery:", e);
                    }
                });
    }

    // Requires the admin cluster. Currently only implemented for the TCP client
    public DeleteIndexResponse deleteIndex(String indexName) {
        DeleteIndexResponse deleteIndexResponse = this.admin().indices().delete(
                new DeleteIndexRequest(indexName)
        ).actionGet();
        return deleteIndexResponse;
    }

    @Override
    public ElasticSearchHelper.ClientType getClientType() {
        return ElasticSearchHelper.ClientType.TCP;
    }

    @Override
    protected BulkProcessor getBulkProcessor() {
        return this.bulkProcessor;
    }

    public Client getClient() {
        return this.client;
    }

    // ADMIN enabled via TCP only //

    public AdminClient admin() {
        return this.getClient().admin();
    }
}
