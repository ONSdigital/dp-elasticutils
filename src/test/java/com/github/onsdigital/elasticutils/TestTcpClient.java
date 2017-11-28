package com.github.onsdigital.elasticutils;

import com.github.onsdigital.elasticutils.client.ElasticSearchClient;
import com.github.onsdigital.elasticutils.client.ElasticSearchTransportClient;
import com.github.onsdigital.elasticutils.models.GeoLocation;
import org.apache.http.HttpStatus;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

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
// Run tests in name ascending order
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestTcpClient
{

    private static final String HOSTNAME = "localhost";
    private static final String DOCUMENT_ID = UUID.randomUUID().toString();

    private ElasticSearchTransportClient<GeoLocation> getClient(TestTcpClient.ElasticSearchPort port) {
        Settings settings = Settings.builder().put("cluster.name", port.getClusterName()).build();
        ElasticSearchTransportClient<GeoLocation> searchClient = null;
        try {
            searchClient = new ElasticSearchTransportClient<GeoLocation>(
                    HOSTNAME, port.getPort(), TestTcpClient.ElasticIndex.TEST.getIndexName(), settings, GeoLocation.class
            );
        } catch (UnknownHostException e) {
            Assert.fail(e.getMessage());
        }
        return searchClient;
    }

    @Test
    public void testAClientConnection() {
        // Test that we can connect to the client

        List<String> ok = new ArrayList<String>() {{
           add("YELLOW");
           add("GREEN");
        }};

        for (TestTcpClient.ElasticSearchPort port : TestTcpClient.ElasticSearchPort.values()) {
            try {
                ElasticSearchTransportClient<GeoLocation> searchClient = getClient(port);

                AdminClient adminClient = searchClient.admin();
                ClusterHealthStatus healthStatus = adminClient.cluster().prepareClusterStats().get().getStatus();
                assertTrue(ok.contains(healthStatus.toString()));
            } catch (Exception e) {
                Assert.fail("Exception in testClientConnection: " + e);
            }
        }
    }

    @Test
    public void testBClientIndex() {
        // Test that we can index new documents

        // Test object to index
        GeoLocation geoLocation = new GeoLocation(DOCUMENT_ID, 51.566407, -3.027560);  // ONS

        for (TestTcpClient.ElasticSearchPort port : TestTcpClient.ElasticSearchPort.values()) {
            try {
                // Here we can just use the generic ElasticSearchClient
                ElasticSearchClient<GeoLocation> searchClient = getClient(port);

                IndexResponse indexResponse = searchClient.indexAndRefresh(geoLocation);
                searchClient.awaitClose(1, TimeUnit.SECONDS);

                // Assert that we got back a 201 response
                assertEquals(HttpStatus.SC_CREATED, indexResponse.status().getStatus());
            } catch (Exception e) {
                Assert.fail("Exception in createTestIndexHttp: " + e);
            }
        }
    }

    @Test
    public void testCClientSearch() {
        // Search
        for (TestTcpClient.ElasticSearchPort port : TestTcpClient.ElasticSearchPort.values()) {
            QueryBuilder qb = QueryBuilders.matchQuery("geoId", DOCUMENT_ID);

            ElasticSearchClient<GeoLocation> searchClient = null;
            SearchHits hits = null;
            try {
                searchClient = getClient(port);

                hits = searchClient.search(qb);
            } catch (IOException e) {
                Assert.fail("Exception in testHttpSearch: " + e);
            }
            List<GeoLocation> geoLocations = searchClient.deserialize(hits);

            assertEquals(1, geoLocations.size());
            assertEquals(DOCUMENT_ID, geoLocations.get(0).getGeoId());
        }
    }

    @Test
    public void testDDeleteIndex() {
        // Cleanup

        // If the test index exists, delete it before we start the tests
        // Delete the index
        for (TestTcpClient.ElasticSearchPort port : TestTcpClient.ElasticSearchPort.values()) {
            ElasticSearchTransportClient<GeoLocation> searchClient = getClient(port);

            DeleteIndexResponse deleteIndexResponse = searchClient.deleteIndex(ElasticIndex.TEST.getIndexName());
            assertTrue(deleteIndexResponse.isAcknowledged());
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
