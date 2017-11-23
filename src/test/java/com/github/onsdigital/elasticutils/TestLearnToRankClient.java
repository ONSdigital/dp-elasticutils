package com.github.onsdigital.elasticutils;

import com.github.onsdigital.elasticutils.ml.client.ElasticSearchLtrRESTClient;
import com.github.onsdigital.elasticutils.ml.features.Feature;
import com.github.onsdigital.elasticutils.ml.features.FeatureSet;
import com.github.onsdigital.elasticutils.ml.features.Template;
import com.github.onsdigital.elasticutils.ml.requests.FeatureSetRequest;
import org.apache.http.HttpStatus;
import org.elasticsearch.client.Response;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;

/**
 * @author sullid (David Sullivan) on 23/11/2017
 * @project dp-elasticutils
 */
public class TestLearnToRankClient {

    private static final String HOSTNAME = "localhost";

    private FeatureSetRequest generateDefaultRequest() {
        Map<String, String> templateMatch = new HashMap<String, String>() {{
            put("title", "{{keywords}}");
            put("overview", "{{keywords}}");
        }};
        Template template = new Template(templateMatch);

        List<String> params = new ArrayList<String>() {{
            add("keywords");
        }};
        Feature feature = new Feature("title_query", params, template);

        List<Feature> featureList = new ArrayList<Feature>() {{
            add(feature);
        }};

        FeatureSet featureSet = new FeatureSet("java_test_feature_set", featureList);

        FeatureSetRequest request = FeatureSetRequest.builder().featureSet(featureSet).build();
        return request;
    }

    public void testCreateFeatureSet() {
        FeatureSetRequest request = generateDefaultRequest();

        ElasticSearchLtrRESTClient client = new ElasticSearchLtrRESTClient(HOSTNAME, Object.class);
        try {
            Response response = client.addFeatureSet(request);
            assertTrue(response.getStatusLine().getStatusCode() == HttpStatus.SC_CREATED);
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
        try {
            client.close();
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    public void testDeleteFeatureSet() {
        FeatureSetRequest request = generateDefaultRequest();

        ElasticSearchLtrRESTClient client = new ElasticSearchLtrRESTClient(HOSTNAME, Object.class);
        try {
            Response response = client.deleteFeatureSet(request);
            assertTrue(response.getStatusLine().getStatusCode() == HttpStatus.SC_OK);
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
        try {
            client.close();
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

}
