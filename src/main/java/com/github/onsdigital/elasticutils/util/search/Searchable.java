package com.github.onsdigital.elasticutils.util.search;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * @author sullid (David Sullivan) on 13/12/2017
 * @project dp-elasticutils
 */
public interface Searchable {

    @JsonIgnore
    ElasticSearchIndex getIndex();

}
