package com.github.onsdigital.elasticutils.ml.client;

import com.github.onsdigital.elasticutils.client.ElasticSearchRESTClient;
import com.github.onsdigital.elasticutils.client.bulk.configuration.BulkProcessorConfiguration;
import com.github.onsdigital.elasticutils.indicies.ElasticIndexNames;
import org.elasticsearch.client.Response;

import java.io.IOException;

/**
 * @author sullid (David Sullivan) on 23/11/2017
 * @project dp-elasticutils
 *
 * A RESTful client designed to work with Learn to Rank
 */
public class ElasticSearchLtrRESTClient<T> extends ElasticSearchRESTClient {
    public ElasticSearchLtrRESTClient(String hostName, ElasticIndexNames indexName, Class returnClass) {
        super(hostName, indexName, returnClass);
    }

    public ElasticSearchLtrRESTClient(String hostName, int http_port, ElasticIndexNames indexName, Class returnClass) {
        super(hostName, http_port, indexName, returnClass);
    }

    public ElasticSearchLtrRESTClient(String hostName, int http_port, ElasticIndexNames indexName, BulkProcessorConfiguration bulkProcessorConfiguration, Class returnClass) {
        super(hostName, http_port, indexName, bulkProcessorConfiguration, returnClass);
    }

    public Response initFeatureStore() throws IOException {
        Response response = super.getLowLevelClient().performRequest(HttpRequestType.PUT.getRequestType(), LtrAPI.LTR_API);
        return response;
    }

    public Response deleteFeatureStore() throws IOException {
        Response response = super.getLowLevelClient().performRequest(HttpRequestType.PUT.getRequestType(), LtrAPI.LTR_API);
        return response;
    }

    public static class LtrAPI {
        private static final String LTR_API = "_ltr";

        public static String getApiEndpoint(LtrAPIEndpoint ltrAPIEndpoint) {
            return LTR_API + "/" + ltrAPIEndpoint.getEndPoint();
        }

    }

    public enum LtrAPIEndpoint {
        FEATURESET("_featureset");

        private String endPoint;

        LtrAPIEndpoint(String endPoint) {
            this.endPoint = endPoint;
        }

        public String getEndPoint() {
            return endPoint;
        }
    }
}
