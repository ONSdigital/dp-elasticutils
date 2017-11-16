package com.github.onsdigital.elasticutils.index;

import com.github.onsdigital.elasticutils.indicies.ElasticIndexNames;

/**
 * @author sullid (David Sullivan) on 16/11/2017
 * @project dp-elasticutils
 *
 * Example of an ElasticIndexNames enum, specifying the full list of indicies
 * used
 */
public enum ElasticIndex implements ElasticIndexNames {
    TEST("test");

    private String name;

    ElasticIndex(String name) {
        this.name = name;
    }

    @Override
    public String getIndexName() {
        return name;
    }
}
