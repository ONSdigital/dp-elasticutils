package com.github.onsdigital.elasticutils.client.generic;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * @author sullid (David Sullivan) on 29/11/2017
 * @project dp-elasticutils
 */
public class ElasticSearchResponse<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticSearchResponse.class);
    private  ObjectMapper mapper;

    private SearchResponse response;
    private Class<T> returnClass;

    public ElasticSearchResponse(SearchResponse response, Class<T> returnClass) {
        this(response, new ObjectMapper(), returnClass);
    }

    public ElasticSearchResponse(SearchResponse response, ObjectMapper mapper, Class<T> returnClass) {
        this.response = response;
        this.mapper = mapper;
        this.returnClass = returnClass;
    }

    public void setMapper(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public SearchResponse getResponse() {
        return response;
    }

    public SearchHits getSearchHits() {
        return this.response.getHits();
    }

    public List<T> entities() {
        List<T> results = new LinkedList<>();

        SearchHits searchHits = this.getSearchHits();

        searchHits.forEach(hit -> {
            try {
                results.add(this.mapper.readValue(hit.getSourceAsString(), returnClass));
            } catch (IOException e) {
                LOGGER.error("Error unmarshalling from json", e);
            }
        });

        return results;
    }

}
