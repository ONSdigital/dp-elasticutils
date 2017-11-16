package com.github.onsdigital.elasticutils;

import com.github.onsdigital.elasticutils.client.ElasticSearchRESTClient;
import com.github.onsdigital.elasticutils.client.ElasticSearchTransportClient;
import com.github.onsdigital.elasticutils.index.ElasticIndex;
import com.github.onsdigital.elasticutils.indicies.ElasticIndexNames;
import com.github.onsdigital.elasticutils.models.GeoLocation;
import com.github.onsdigital.elasticutils.util.ElasticSearchHelper;
import org.apache.http.HttpStatus;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Assert;
import org.junit.Test;

import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

/**
 * @author sullid (David Sullivan) on 16/11/2017
 * @project dp-elasticutils
 */
public class TestClient {

    private static final String HOSTNAME = "localhost";
    private static final String TYPE = "DOCUMENT";

    @Test
    public void testTcpClientConnection() throws InterruptedException {
        ElasticSearchTransportClient client = null;
        try {
             client = new ElasticSearchTransportClient(HOSTNAME, ElasticSearchHelper.DEFAULT_TCP_PORT);
        } catch (UnknownHostException e) {
            Assert.fail("Exception: " + e);
        }

        ClusterHealthResponse healthResponse = client.admin().cluster().prepareHealth().get();
        int status = healthResponse.status().getStatus();
        assertEquals(HttpStatus.SC_OK, status);

        GeoLocation geoLocation = new GeoLocation(51.566407, -3.027560);  // ONS

        client.index(ElasticIndex.TEST, geoLocation);

        // The Bulk Insert is asynchronous, we give ElasticSearch some time to do the insert:
        client.awaitClose(1, TimeUnit.SECONDS);
    }

    @Test
    public void testRestClientConnection() throws InterruptedException {
        ElasticSearchRESTClient client = new ElasticSearchRESTClient(HOSTNAME, ElasticSearchHelper.DEFAULT_HTTP_PORT);

        GeoLocation geoLocation = new GeoLocation(51.566407, -3.027560);  // ONS

        client.index(ElasticIndex.TEST, geoLocation);

        // The Bulk Insert is asynchronous, we give ElasticSearch some time to do the insert:
        client.awaitClose(1, TimeUnit.SECONDS);
    }

    public IndexRequest createIndexRequest(ElasticIndexNames indexNames, byte[] messageBytes) {
        IndexRequest indexRequest = new IndexRequest(indexNames.getIndexName())
                .source(messageBytes, XContentType.JSON)
                .type(TYPE);

        return indexRequest;
    }

}
