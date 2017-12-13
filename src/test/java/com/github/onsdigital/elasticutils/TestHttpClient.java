package com.github.onsdigital.elasticutils;

import com.github.onsdigital.elasticutils.client.bulk.configuration.BulkProcessorConfiguration;
import com.github.onsdigital.elasticutils.client.generic.ElasticSearchClient;
import com.github.onsdigital.elasticutils.client.generic.RestSearchClient;
import com.github.onsdigital.elasticutils.client.http.SimpleRestClient;
import com.github.onsdigital.elasticutils.client.type.DefaultDocumentTypes;
import com.github.onsdigital.elasticutils.models.GeoLocation;
import com.github.onsdigital.elasticutils.models.SearchIndex;
import com.github.onsdigital.elasticutils.util.ElasticSearchHelper;
import org.apache.http.HttpStatus;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.main.MainResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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

public class TestHttpClient {

    private static final String HOSTNAME = "localhost";
    private static final String DOCUMENT_ID = UUID.randomUUID().toString();

    private RestSearchClient<GeoLocation> getClient(ElasticSearchPort port) {
        SimpleRestClient client = ElasticSearchHelper.getRestClient(HOSTNAME, port.getPort());
        BulkProcessorConfiguration configuration = ElasticSearchHelper.getDefaultBulkProcessorConfiguration();
        RestSearchClient<GeoLocation> searchClient = new RestSearchClient<GeoLocation>(client, configuration);
        return searchClient;
    }

    @Before
    public void createIndex() {
        // Test that we can index new documents

        // Test object to index
        GeoLocation geoLocation = new GeoLocation(DOCUMENT_ID, 51.566407, -3.027560);  // ONS

        for (ElasticSearchPort port : ElasticSearchPort.values()) {
            try {
                // Here we can just use the generic ElasticSearchClient
                ElasticSearchClient<GeoLocation> searchClient = getClient(port);

                IndexRequest request = searchClient.prepareIndex()
                        .setIndex(geoLocation.getIndex().getIndexName())
                        .setType(DefaultDocumentTypes.DOCUMENT.getType())
                        .setSource(geoLocation)
                        .request()
                        .setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);;

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
        for (ElasticSearchPort port : ElasticSearchPort.values()) {
            RestSearchClient client = getClient(port);
            try {
                boolean success = client.dropIndex(SearchIndex.TEST.getIndexName());
                assertTrue(success);
            } catch (IOException e) {
                Assert.fail("Failed to delete test index: " + e);
            }
        }
    }

    @Test
    public void testClientConnection() {
        // Test that we can connect to the client

        for (ElasticSearchPort port : ElasticSearchPort.values()) {
            try {
                RestSearchClient<GeoLocation> searchClient = getClient(port);

                MainResponse response = searchClient.info();
                assertTrue(response.isAvailable());
            } catch (Exception e) {
                Assert.fail("Exception in testClientConnection: " + e);
            }
        }
    }

    @Test
    public void testClientSearch() {
        // Search
        for (ElasticSearchPort port : ElasticSearchPort.values()) {
            QueryBuilder qb = QueryBuilders.matchQuery("geoId", DOCUMENT_ID);

            ElasticSearchClient<GeoLocation> searchClient = getClient(port);

            List<GeoLocation> geoLocations = null;
            try {
                geoLocations = GeoLocation.searcher(searchClient).search(qb, DefaultDocumentTypes.DOCUMENT);
            } catch (IOException e) {
                Assert.fail(e.getMessage());
            }

            assertEquals(1, geoLocations.size());
            assertEquals(DOCUMENT_ID, geoLocations.get(0).getGeoId());
        }
    }

//    @Test
//    public void testIndexWithRefreshInterval() {
//        // Test object to index
//        GeoLocation geoLocation = new GeoLocation(DOCUMENT_ID, 51.566407, -3.027560);  // ONS
//
//        for (ElasticSearchPort port : ElasticSearchPort.values()) {
//            RestSearchClient<GeoLocation> searchClient = null;
//            try {
//                searchClient = getClient(port);
//
//                searchClient.bulkIndexWithRefreshInterval(Arrays.asList(geoLocation));
//                searchClient.awaitClose(1, TimeUnit.SECONDS);
//            } catch (IOException e) {
//                Assert.fail("Exception in testHttpIndexSearchAndDelete: " + e);
//            } catch (InterruptedException e) {
//                Assert.fail(e.getMessage());
//            }
//        }
//    }

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
