package com.github.onsdigital.elasticutils.ml.requests;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.onsdigital.elasticutils.ml.client.ElasticSearchLtrRESTClient;
import com.github.onsdigital.elasticutils.ml.features.Feature;
import com.github.onsdigital.elasticutils.ml.features.FeatureSet;
import com.github.onsdigital.elasticutils.ml.features.Template;
import org.elasticsearch.client.Response;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author sullid (David Sullivan) on 23/11/2017
 * @project dp-elasticutils
 */
public class FeatureSetRequest {

    private FeatureSet featureSet;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Validation validation;

    @JsonIgnore
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private FeatureSetRequest() {

    }

    public FeatureSetRequest(FeatureSet featureSet) {
        this.featureSet = featureSet;
    }

    public FeatureSetRequest(FeatureSet featureSet, Validation validation) {
        this(featureSet);
        this.validation = validation;
    }

    @JsonIgnore
    public static FeatureSetRequestBuilder builder() {
        return new FeatureSetRequestBuilder();
    }

    @JsonProperty("featureset")
    public FeatureSet getFeatureSet() {
        return featureSet;
    }

    public void setFeatureSet(FeatureSet featureSet) {
        this.featureSet = featureSet;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Validation getValidation() {
        return validation;
    }

    public void setValidation(Validation validation) {
        this.validation = validation;
    }

    @JsonIgnore
    public String toJson() throws JsonProcessingException {
        return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(this);
    }

    private static class FeatureSetRequestBuilder {
        @JsonIgnore
        private FeatureSetRequest request = new FeatureSetRequest();

        @JsonIgnore
        public FeatureSetRequestBuilder featureSet(FeatureSet featureSet) {
            this.request.setFeatureSet(featureSet);
            return this;
        }

        @JsonIgnore
        public FeatureSetRequestBuilder validation(Validation validation) {
            this.request.setValidation(validation);
            return this;
        }

        @JsonIgnore
        public FeatureSetRequest build() {
            return this.request;
        }

    }

    public static void main(String[] args) throws IOException {
        Map<String, String> templateMatch = new HashMap<String, String>() {{
            put("title", "{{keywords}}");
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

        ElasticSearchLtrRESTClient client = new ElasticSearchLtrRESTClient("localhost", Object.class);
        Response response = client.addFeatureSet(request);
        System.out.println(response);

        Response deleteResponse = client.deleteFeatureSet(request);
        System.out.println(deleteResponse);
    }

}
