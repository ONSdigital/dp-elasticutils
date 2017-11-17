package com.github.onsdigital.elasticutils;

import com.github.onsdigital.elasticutils.client.ElasticSearchClient;
import com.github.onsdigital.elasticutils.client.ElasticSearchRESTClient;
import com.github.onsdigital.elasticutils.client.ElasticSearchTransportClient;
import com.github.onsdigital.elasticutils.index.ElasticIndex;
import com.github.onsdigital.elasticutils.models.GeoLocation;
import org.apache.http.HttpStatus;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Response;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

/**
 * @author sullid (David Sullivan) on 16/11/2017
 * @project dp-elasticutils
 *
 * Series of tests to ensure indexing/searching/deletion is working for both the
 * TCP and HTTP transport clients.
 */
public class TestClient {

    private static final String HOSTNAME = "localhost";

    private static final String ID_HTTP = "1";
    private static final String ID_TCP = "2";

    @Test
    public void createTestIndexHttp() {
        // Index some test data via http

        try {
            ElasticSearchClient<GeoLocation> searchClient = new ElasticSearchRESTClient<GeoLocation>(
                    HOSTNAME, ElasticIndex.TEST, GeoLocation.class
            );

            GeoLocation testGeoLocation = new GeoLocation(ID_HTTP, 51.566407, -3.027560);  // ONS

            IndexResponse indexResponse = searchClient.indexAndRefresh(testGeoLocation);

            searchClient.awaitClose(1, TimeUnit.SECONDS);

            assertEquals(HttpStatus.SC_CREATED, indexResponse.status().getStatus());
        } catch (Exception e) {
            Assert.fail("Exception in createTestIndexHttp: " + e);
        }
    }

    @Test
    public void testHttpIndexSearchAndDelete() {
        // Create

        for (Ports port : Ports.values()) {

            System.out.println(String.format("Connecting to instance: %s", port));

            createTestIndexHttp();

            ElasticSearchRESTClient<GeoLocation> searchClient = new ElasticSearchRESTClient<GeoLocation>(
                    HOSTNAME, port.getPort(), ElasticIndex.TEST, GeoLocation.class
            );

            // Search
            QueryBuilder qb = QueryBuilders.matchQuery("geoId", ID_HTTP);
            SearchHits hits = null;
            try {
                hits = searchClient.search(qb);
            } catch (IOException e) {
                Assert.fail("Exception in testHttpSearch: " + e);
            }
            List<GeoLocation> geoLocations = searchClient.deserialize(hits);

            assertEquals(1, geoLocations.size());
            assertEquals(ID_HTTP, geoLocations.get(0).getGeoId());

            // Delete the record
            String id = hits.getAt(0).getId();
            DeleteResponse deleteResponse = null;
            try {
                deleteResponse = searchClient.deleteById(id);
            } catch (IOException e) {
                Assert.fail("Exception in testHttpIndexSearchAndDelete: " + e);
            }

            assertEquals(HttpStatus.SC_OK, deleteResponse.status().getStatus());

            // Delete the index
            Response response = null;
            try {
                response = searchClient.deleteIndex(ElasticIndex.TEST);

                assertEquals(HttpStatus.SC_OK, response.getStatusLine().getStatusCode());
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }

    @Test
    public void createTestIndexTcp() {
        // Index some test data via http

        try {
            ElasticSearchClient<GeoLocation> searchClient = new ElasticSearchTransportClient<GeoLocation>(
                    HOSTNAME, ElasticIndex.TEST, GeoLocation.class
            );

            GeoLocation testGeoLocation = new GeoLocation(ID_TCP, 51.566407, -3.027560);  // ONS

            searchClient.indexAndRefresh(testGeoLocation);
        } catch (Exception e) {
            Assert.fail("Exception in createTestIndexTcp: " + e);
        }
    }


    public enum Ports {
        ES6(9200),
        ES5(9205);

        private int port;

        Ports(int port) {
            this.port = port;
        }

        public int getPort() {
            return port;
        }
    }


}
