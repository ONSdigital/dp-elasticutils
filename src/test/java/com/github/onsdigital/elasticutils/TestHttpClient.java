package com.github.onsdigital.elasticutils;

import com.github.onsdigital.elasticutils.client.ElasticSearchClient;
import com.github.onsdigital.elasticutils.client.ElasticSearchRESTClient;
import com.github.onsdigital.elasticutils.client.ElasticSearchTransportClient;
import com.github.onsdigital.elasticutils.index.ElasticIndex;
import com.github.onsdigital.elasticutils.models.GeoLocation;
import org.apache.http.HttpStatus;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.main.MainResponse;
import org.elasticsearch.client.Response;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.junit.*;
import org.junit.runners.MethodSorters;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author sullid (David Sullivan) on 16/11/2017
 * @project dp-elasticutils
 *
 * Series of tests to ensure indexing/searching/deletion is working for both the
 * TCP and HTTP transport clients.
 */

// Run tests in name ascending order
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestHttpClient {

    private static final String HOSTNAME = "localhost";
    private static final String DOCUMENT_ID = UUID.randomUUID().toString();

    private ElasticSearchRESTClient<GeoLocation> getClient(ElasticSearchPort port) {
        ElasticSearchRESTClient<GeoLocation> searchClient = new ElasticSearchRESTClient<GeoLocation>(
                HOSTNAME, port.getPort(), ElasticIndex.TEST, GeoLocation.class
        );
        return searchClient;
    }

    @Test
    public void testAClientConnection() {
        // Test that we can connect to the client

        for (ElasticSearchPort port : ElasticSearchPort.values()) {
            try {
                ElasticSearchRESTClient<GeoLocation> searchClient = getClient(port);

                MainResponse response = searchClient.info();
                assertTrue(response.isAvailable());
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

        for (ElasticSearchPort port : ElasticSearchPort.values()) {
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
        for (ElasticSearchPort port : ElasticSearchPort.values()) {
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
        for (ElasticSearchPort port : ElasticSearchPort.values()) {
            Response response = null;
            try {
                ElasticSearchRESTClient<GeoLocation> searchClient = getClient(port);

                response = searchClient.deleteIndex(ElasticIndex.TEST);
                assertEquals(HttpStatus.SC_OK, response.getStatusLine().getStatusCode());
            } catch (IOException e) {
                Assert.fail("Exception in testHttpIndexSearchAndDelete: " + e);
            }
        }
    }


    /*
    This enum is used for testing only with the supplied docker containers
     */
    public enum ElasticSearchPort {
        ElasticSearch_6_0_0(9200),
        ElasticSearch_5_5_0(9205);

        private int port;

        ElasticSearchPort(int port) {
            this.port = port;
        }

        public int getPort() {
            return port;
        }
    }


}
