package com.github.onsdigital.elasticutils.models;

import com.github.onsdigital.elasticutils.util.search.ElasticSearchIndex;

/**
 * @author sullid (David Sullivan) on 13/12/2017
 * @project dp-elasticutils
 */
public enum SearchIndex implements ElasticSearchIndex {
    TEST("test");

    private String indexName;

    SearchIndex(String indexName) {
        this.indexName = indexName;
    }

    @Override
    public String getIndexName() {
        return this.indexName;
    }
}
