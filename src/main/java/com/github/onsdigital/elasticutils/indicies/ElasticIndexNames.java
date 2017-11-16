package com.github.onsdigital.elasticutils.indicies;

/**
 * @author sullid (David Sullivan) on 16/11/2017
 * @project dp-elasticutils
 *
 * Interface to define the behaviour of Index enums.
 * Used by the ElasticSearchClient
 */
public interface ElasticIndexNames {
    String getIndexName();
}
