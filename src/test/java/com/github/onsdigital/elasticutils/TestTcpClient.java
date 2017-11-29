package com.github.onsdigital.elasticutils;

import com.github.onsdigital.elasticutils.client.bulk.configuration.BulkProcessorConfiguration;
import com.github.onsdigital.elasticutils.client.generic.ElasticSearchClient;
import com.github.onsdigital.elasticutils.client.generic.ElasticSearchResponse;
import com.github.onsdigital.elasticutils.client.generic.RestSearchClient;
import com.github.onsdigital.elasticutils.client.generic.TransportSearchClient;
import com.github.onsdigital.elasticutils.client.type.DocumentType;
import com.github.onsdigital.elasticutils.models.GeoLocation;
import com.github.onsdigital.elasticutils.util.ElasticSearchHelper;
import org.apache.http.HttpStatus;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.main.MainResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author sullid (David Sullivan) on 28/11/2017
 * @project dp-elasticutils
 */
public class TestTcpClient
{

    private static final String HOSTNAME = "localhost";
    private static final String DOCUMENT_ID = UUID.randomUUID().toString();

    private TransportSearchClient<GeoLocation> getClient(TestTcpClient.ElasticSearchPort port) {
        Settings settings = Settings.builder().put("cluster.name", port.getClusterName()).build();
        BulkProcessorConfiguration configuration = ElasticSearchHelper.getDefaultBulkProcessorConfiguration();

        try {
            TransportClient client = ElasticSearchHelper.getTransportClient(HOSTNAME, port.getPort(), settings);

            TransportSearchClient<GeoLocation> searchClient = new TransportSearchClient<GeoLocation>(
                    client, ElasticIndex.TEST.getIndexName(), configuration, GeoLocation.class
            );
            return searchClient;
        } catch (UnknownHostException e) {
            Assert.fail(e.getMessage());
        }
        return null;
    }

    @Before
    public void createIndex() {
        // Test that we can index new documents

        // Test object to index
        GeoLocation geoLocation = new GeoLocation(DOCUMENT_ID, 51.566407, -3.027560);  // ONS

        for (TestTcpClient.ElasticSearchPort port : TestTcpClient.ElasticSearchPort.values()) {
            try {
                // Here we can just use the generic ElasticSearchClient
                TransportSearchClient<GeoLocation> searchClient = getClient(port);

                IndexRequest request = searchClient.prepareIndex()
                        .setIndex(TestHttpClient.ElasticIndex.TEST.getIndexName())
                        .setType(DocumentType.DOCUMENT.getType())
                        .setSource(geoLocation)
                        .request()
                        .setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);

                IndexResponse indexResponse = searchClient.index(request);
                searchClient.awaitClose(1, TimeUnit.SECONDS);

                // Assert that we got back a 201 response
                assertEquals(HttpStatus.SC_CREATED, indexResponse.status().getStatus());
            } catch (Exception e) {
                e.printStackTrace();
                Assert.fail("Exception in createTestIndexHttp: " + e);
            }
        }
    }

    @After
    public void deleteIndex() {
        for (TestTcpClient.ElasticSearchPort port : TestTcpClient.ElasticSearchPort.values()) {
            TransportSearchClient<GeoLocation> client = getClient(port);

            DeleteIndexResponse response = client.dropIndex();
            assertTrue(response.isAcknowledged());
        }
    }

    @Test
    public void testClientConnection() {
        // Test that we can connect to the client

        List<String> ok = new ArrayList<String>() {{
            add("YELLOW");
            add("GREEN");
        }};

        for (TestTcpClient.ElasticSearchPort port : TestTcpClient.ElasticSearchPort.values()) {
            try {
                TransportSearchClient<GeoLocation> searchClient = getClient(port);

                AdminClient adminClient = searchClient.admin();
                ClusterHealthStatus healthStatus = adminClient.cluster().prepareClusterStats().get().getStatus();
                assertTrue(ok.contains(healthStatus.toString()));
            } catch (Exception e) {
                Assert.fail("Exception in testClientConnection: " + e);
            }
        }
    }

    @Test
    public void testClientSearch() {
        // Search
        for (TestTcpClient.ElasticSearchPort port : TestTcpClient.ElasticSearchPort.values()) {
            QueryBuilder qb = QueryBuilders.matchQuery("geoId", DOCUMENT_ID);

            ElasticSearchClient<GeoLocation> searchClient = getClient(port);

            SearchRequest request = searchClient.prepareSearch()
                    .setTypes(DocumentType.DOCUMENT.getType())
                    .setQuery(qb)
                    .setExplain(true)
                    .request();

            ElasticSearchResponse<GeoLocation> response = null;

            try {
                response = searchClient.search(request);
            } catch (IOException e) {
                Assert.fail("Exception in testHttpSearch: " + e);
            }
            List<GeoLocation> geoLocations = response.entities();

            assertEquals(1, geoLocations.size());
            assertEquals(DOCUMENT_ID, geoLocations.get(0).getGeoId());
        }
    }


    /*
    This enum is used for testing only with the supplied docker containers
     */
    public enum ElasticSearchPort {
        ElasticSearch_6_0_0(9300, "docker-cluster"),
        ElasticSearch_5_5_0(9305, "docker-cluster2");

        private int port;
        private String clusterName;

        ElasticSearchPort(int port, String clusterName) {
            this.port = port;
            this.clusterName = clusterName;
        }

        public int getPort() {
            return port;
        }

        public String getClusterName() {
            return clusterName;
        }
    }

    public enum ElasticIndex {
        TEST("test");

        private String indexName;

        ElasticIndex(String indexName) {
            this.indexName = indexName;
        }

        public String getIndexName() {
            return indexName;
        }
    }

}
