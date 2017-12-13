package com.github.onsdigital.elasticutils.client;

import com.github.onsdigital.elasticutils.action.delete.SimpleDeleteRequestBuilder;
import com.github.onsdigital.elasticutils.action.index.SimpleIndexRequestBuilder;
import com.github.onsdigital.elasticutils.action.search.SimpleSearchRequestBuilder;
import com.github.onsdigital.elasticutils.client.type.DocumentType;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * @author sullid (David Sullivan) on 16/11/2017
 * @project dp-elasticutils
 */
public interface DefaultSearchClient<T> extends AutoCloseable {

    IndexResponse index(IndexRequest request) throws IOException;

    void bulk(String index, DocumentType documentType, T entity);

    void bulk(String index, DocumentType documentType, List<T> entities);

    void bulk(String index, DocumentType documentType, Stream<T> entities);

    SimpleIndexRequestBuilder prepareIndex();

    default SimpleSearchRequestBuilder prepareSearch(String index) {
        return prepareSearch(new String[]{
                index
        });
    }

    SimpleSearchRequestBuilder prepareSearch(String[] indices);

    SimpleDeleteRequestBuilder prepareDelete();

    void flush();

    boolean awaitClose(long timeout, TimeUnit unit) throws InterruptedException;

}
