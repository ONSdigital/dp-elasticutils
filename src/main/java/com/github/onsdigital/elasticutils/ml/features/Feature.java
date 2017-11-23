package com.github.onsdigital.elasticutils.ml.features;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.List;

/**
 * @author sullid (David Sullivan) on 23/11/2017
 * @project dp-elasticutils
 *
 * Simple feature for Elasticsearch LTR
 */
public class Feature {

    private String name;
    private List<String> params;
    private String templateLanguage;
    private Template template;

    @JsonIgnore
    private static final String DEFAULT_TEMPLATING_LANGUAGE = "mustache";

    public Feature (String name, List<String> params, Template template) {
        this(name, params, DEFAULT_TEMPLATING_LANGUAGE, template);
    }

    public Feature(String name, List<String> params, String templateLanguage, Template template) {
        this.name = name;
        this.params = params;
        this.templateLanguage = templateLanguage;
        this.template = template;
    }

    public String getName() {
        return name;
    }

    public List<String> getParams() {
        return params;
    }

    public String getTemplateLanguage() {
        return templateLanguage;
    }

    public Template getTemplate() {
        return template;
    }
}
