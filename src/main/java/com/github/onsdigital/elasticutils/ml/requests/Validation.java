package com.github.onsdigital.elasticutils.ml.requests;

import java.util.Map;

/**
 * @author sullid (David Sullivan) on 23/11/2017
 * @project dp-elasticutils
 */
public class Validation {

    private Map<String, String> params;
    private String index;

    public Validation(Map<String, String> params, String index) {
        this.params = params;
        this.index = index;
    }

    private Validation() {
        // For Jackson
    }

    public Map<String, String> getParams() {
        return params;
    }

    public String getIndex() {
        return index;
    }
}
