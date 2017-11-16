package com.github.onsdigital.elasticutils;

import com.github.onsdigital.elasticutils.index.ElasticIndex;
import com.github.onsdigital.elasticutils.indicies.ElasticIndexNames;
import com.github.onsdigital.elasticutils.models.GeoLocation;
import com.github.onsdigital.elasticutils.util.ElasticSearchHelper;
import com.github.onsdigital.elasticutils.util.JsonUtils;
import org.apache.http.HttpStatus;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Optional;

import static org.junit.Assert.assertEquals;

/**
 * @author sullid (David Sullivan) on 16/11/2017
 * @project dp-elasticutils
 */
public class TestClient {

    private static final String HOSTNAME = "localhost";
    private static final String TYPE = "DOCUMENT";

    @Test
    public void testTcpClientConnection() {
        Client tcpClient = null;
        try {
             tcpClient = ElasticSearchHelper.getTransportClient(HOSTNAME);
        } catch (UnknownHostException e) {
            Assert.fail("Exception: " + e);
        }

        ClusterHealthResponse healthResponse = tcpClient.admin().cluster().prepareHealth().get();
        int status = healthResponse.status().getStatus();
        assertEquals(HttpStatus.SC_OK, status);
    }

    @Test
    public void testRestClientConnection() {
        RestHighLevelClient restClient = ElasticSearchHelper.getRestClient(HOSTNAME);
        ElasticIndex index = ElasticIndex.TEST;

        GeoLocation geoLocation = new GeoLocation(51.566407, -3.027560);  // ONS

        Optional<byte[]> messageBytes = JsonUtils.convertJsonToBytes(geoLocation);
        if (messageBytes.isPresent()) {
            IndexRequest request = createIndexRequest(index, messageBytes.get());

            try {
                IndexResponse indexResponse = restClient.index(request);
                assertEquals(HttpStatus.SC_CREATED, indexResponse.status().getStatus());
            } catch (IOException e) {
                Assert.fail("Exception: " + e);
            }
        }
    }

    public IndexRequest createIndexRequest(ElasticIndexNames indexNames, byte[] messageBytes) {
        IndexRequest indexRequest = new IndexRequest(indexNames.getIndexName())
                .source(messageBytes, XContentType.JSON)
                .type(TYPE);

        return indexRequest;
    }

}
