package com.github.onsdigital.elasticutils.ml.features;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.*;

/**
 * @author sullid (David Sullivan) on 23/11/2017
 * @project dp-elasticutils
 *
 * Simple class to represent a FeatureSet in Elasticsearch LTR
 */
public class FeatureSet {

    private String name;
    private List<Feature> featureList;

    @JsonIgnore
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public FeatureSet(String name, List<Feature> featureList) {
        this.name = name;
        this.featureList = featureList;
    }

    @JsonIgnore
    public String getName() {
        return name;
    }

    @JsonIgnore
    public List<Feature> getFeatureList() {
        return featureList;
    }

    @JsonProperty("featureset")
    public Map<String, Object> getFeatureSet() {
        Map<String, Object> featureset = new LinkedHashMap<String, Object>() {{
            put("name", getName());
            put("features", getFeatureList());
        }};
        return featureset;
    }

    @JsonIgnore
    public String toJson() throws JsonProcessingException {
        String json = MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(this);
        return json;
    }

    public static void main(String args[]) {
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

        FeatureSet featureSet = new FeatureSet("test_feature_set", featureList);
        try {
            System.out.println(featureSet.toJson());
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }
}
