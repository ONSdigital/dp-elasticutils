package com.github.onsdigital.elasticutils.ml.features;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.HashMap;
import java.util.Map;

/**
 * @author sullid (David Sullivan) on 23/11/2017
 * @project dp-elasticutils
 */
public class Template {

    private Map<String, String> match;

    public Template() {
        this.match = new HashMap<>();
    }

    public Template(Map<String, String> match) {
        this.match = match;
    }

    public Map<String, String> getMatch() {
        return match;
    }
}
