package com.github.onsdigital.elasticutils.client;

import com.github.onsdigital.elasticutils.client.bulk.configuration.BulkProcessorConfiguration;
import com.github.onsdigital.elasticutils.util.ElasticSearchHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
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

    public ElasticSearchTransportClient(String hostName, String indexName, Class<T> returnClass) throws UnknownHostException {
        this(hostName, ElasticSearchHelper.DEFAULT_TCP_PORT, indexName, returnClass);
    }

    public ElasticSearchTransportClient(String hostName, int transport_port, String indexName, Class<T> returnClass) throws UnknownHostException {
        this(hostName, transport_port, indexName, Settings.EMPTY, returnClass);
    }

    public ElasticSearchTransportClient(String hostName, int transport_port, String indexName, Settings settings, Class<T> returnClass) throws UnknownHostException {
        this(hostName, transport_port, indexName, settings, ElasticSearchHelper.getDefaultBulkProcessorConfiguration(), returnClass);

    }

    public ElasticSearchTransportClient(String hostName, int transport_port, String indexName, Settings settings,
                                        BulkProcessorConfiguration bulkProcessorConfiguration, Class<T> returnClass) throws UnknownHostException {
        super(hostName, transport_port, indexName, bulkProcessorConfiguration, returnClass);
        this.client = ElasticSearchHelper.getTransportClient(super.hostName, super.port, settings);
        this.bulkProcessor = super.bulkProcessorConfiguration.build(client);
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
    public boolean indexExists(String indexName) {
        IndicesExistsRequest request = new IndicesExistsRequest(indexName);

        IndicesExistsResponse response = this.admin().indices().exists(request).actionGet();
        return response.isExists();
    }

    @Override
    public IndexRequest createIndexRequest(byte[] messageBytes) {
        return this.createIndexRequest(messageBytes, XContentType.JSON);
    }

    @Override
    public IndexRequest createIndexRequest(byte[] messageBytes, XContentType xContentType) {
        return this.client.prepareIndex()
                .setIndex(super.indexName)
                .setType(this.documentType.getDocumentType())
                .setSource(messageBytes, xContentType)
                .request();
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
