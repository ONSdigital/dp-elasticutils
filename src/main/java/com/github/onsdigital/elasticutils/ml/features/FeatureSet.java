package com.github.onsdigital.elasticutils.ml.features;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;

/**
 * @author sullid (David Sullivan) on 23/11/2017
 * @project dp-elasticutils
 *
 * Simple class to represent a FeatureSet in Elasticsearch LTR
 */
public class FeatureSet {

    private String name;
    private List<Feature> featureList;

    public FeatureSet(String name) {
        this.name = name;
        this.featureList = new ArrayList<>();
    }

    public FeatureSet(String name, List<Feature> featureList) {
        this.name = name;
        this.featureList = featureList;
    }

    public String getName() {
        return name;
    }

    @JsonProperty("features")
    public List<Feature> getFeatureList() {
        return featureList;
    }
}
