package com.github.onsdigital.elasticutils.ml.client;

import com.github.onsdigital.elasticutils.client.ElasticSearchRESTClient;
import com.github.onsdigital.elasticutils.client.bulk.configuration.BulkProcessorConfiguration;
import com.github.onsdigital.elasticutils.ml.requests.FeatureSetRequest;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * @author sullid (David Sullivan) on 23/11/2017
 * @project dp-elasticutils
 *
 * A RESTful client designed to work with Learn to Rank
 */
public class ElasticSearchLtrRESTClient<T> extends ElasticSearchRESTClient {
    public ElasticSearchLtrRESTClient(String hostName, Class returnClass) {
        super(hostName, LearnToRankAPI.LTR_API, returnClass);
    }

    public ElasticSearchLtrRESTClient(String hostName, int http_port, Class returnClass) {
        super(hostName, http_port, LearnToRankAPI.LTR_API, returnClass);
    }

    public ElasticSearchLtrRESTClient(String hostName, int http_port, BulkProcessorConfiguration bulkProcessorConfiguration, Class returnClass) {
        super(hostName, http_port, LearnToRankAPI.LTR_API, bulkProcessorConfiguration, returnClass);
    }

    public Response initFeatureStore() throws IOException {
        Response response = super.getLowLevelClient().performRequest(HttpRequestType.PUT.getRequestType(), indexName);
        return response;
    }

    public Response addFeatureSet(FeatureSetRequest request) throws IOException {
        RestClient client = super.getLowLevelClient();
        Map<String, String> params = Collections.emptyMap();
        String requestJson = request.toJson();

        String api = LearnToRankAPI.getFeatureApi(request.getFeatureSet().getName());

        HttpEntity entity = new NStringEntity(requestJson, ContentType.APPLICATION_JSON);
        Response response = client.performRequest(HttpRequestType.POST.getRequestType(), api, params, entity);
        return response;
    }

    public Response deleteFeatureSet(FeatureSetRequest request) throws IOException {
        RestClient client = super.getLowLevelClient();

        String api = LearnToRankAPI.getFeatureApi(request.getFeatureSet().getName());
        Response response = client.performRequest(HttpRequestType.DELETE.getRequestType(), api);
        return response;
    }

    public Response deleteFeatureStore() throws IOException {
        Response response = super.getLowLevelClient().performRequest(HttpRequestType.DELETE.getRequestType(), LearnToRankAPI.LTR_API);
        return response;
    }

    public static class LearnToRankAPI {
        private static final String LTR_API = "_ltr";

        public static String getFeatureApi(String featureName) {
            StringBuilder sb = new StringBuilder(LTR_API)
                    .append("/")
                    .append(LeanToRankAPIEndpoint.FEATURESET.getEndPoint())
                    .append("/")
                    .append(featureName);
            return sb.toString();
        }

    }

    public enum LeanToRankAPIEndpoint {
        FEATURESET("_featureset");

        private String endPoint;

        LeanToRankAPIEndpoint(String endPoint) {
            this.endPoint = endPoint;
        }

        public String getEndPoint() {
            return endPoint;
        }
    }
}
