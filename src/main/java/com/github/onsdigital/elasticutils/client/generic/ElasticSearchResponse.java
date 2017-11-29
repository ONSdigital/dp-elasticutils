package com.github.onsdigital.elasticutils.client.generic;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author sullid (David Sullivan) on 29/11/2017
 * @project dp-elasticutils
 */
public class ElasticSearchResponse<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticSearchResponse.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private SearchResponse response;
    private Class<T> returnClass;

    protected ElasticSearchResponse(SearchResponse response, Class<T> returnClass) {
        this.response = response;
        this.returnClass = returnClass;
    }

    public SearchResponse getResponse() {
        return response;
    }

    public SearchHits getSearchHits() {
        return this.response.getHits();
    }

    public List<T> entities() {
        List<T> results = new ArrayList<>();

        SearchHits searchHits = this.getSearchHits();

        searchHits.forEach(hit -> {
            try {
                results.add(MAPPER.readValue(hit.getSourceAsString(), returnClass));
            } catch (IOException e) {
                LOGGER.error("Error unmarshalling from json", e);
            }
        });

        return results;
    }

}
