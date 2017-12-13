package com.github.onsdigital.elasticutils.util;

import com.github.onsdigital.elasticutils.client.generic.ElasticSearchClient;
import com.github.onsdigital.elasticutils.client.generic.ElasticSearchResponse;
import com.github.onsdigital.elasticutils.client.type.DefaultDocumentTypes;
import com.github.onsdigital.elasticutils.client.type.DocumentType;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.io.IOException;
import java.util.List;

/**
 * @author sullid (David Sullivan) on 13/12/2017
 * @project dp-elasticutils
 */
public class ObjectSearcher<T> {

    private ElasticSearchClient<T> searchClient;
    private String index;
    private Class<T> returnClass;

    public ObjectSearcher(ElasticSearchClient<T> searchClient, String index, Class<T> returnClass) {
        this.searchClient = searchClient;
        this.index = index;
        this.returnClass = returnClass;
    }

    public T findOne(String id) throws IOException {
        return this.findOne(id, DefaultDocumentTypes.DOCUMENT);
    }

    public T findOne(String id, DocumentType documentType) throws IOException {
        QueryBuilder qb = QueryBuilders.termQuery("_id", id);
        List<T> results = this.search(qb, documentType);
        if (results.size() != 1) {
            throw new RuntimeException(String.format("Found duplicate ids for id and document type: %s:%s", id, documentType.getType()));
        }
        return results.get(0);
    }

    public List<T> search() throws IOException {
        return this.search(QueryBuilders.matchAllQuery(), DefaultDocumentTypes.DOCUMENT);
    }

    public List<T> search(QueryBuilder qb, DocumentType documentType) throws IOException {
        SearchRequest request = searchClient.prepareSearch(this.index)
                .setTypes(documentType.getType())
                .setQuery(qb)
                .request();
        return this.search(request);
    }

    public List<T> search(SearchRequest request) throws IOException {
        SearchResponse response = this.searchClient.search(request);
        ElasticSearchResponse<T> elasticSearchResponse = new ElasticSearchResponse<>(response, this.returnClass);

        return elasticSearchResponse.entities();
    }

}
