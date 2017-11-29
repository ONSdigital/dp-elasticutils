package com.github.onsdigital.elasticutils.client.http;

import com.github.onsdigital.elasticutils.action.delete.SimpleDeleteRequestBuilder;
import com.github.onsdigital.elasticutils.action.index.SimpleIndexRequestBuilder;
import com.github.onsdigital.elasticutils.action.search.SimpleSearchRequestBuilder;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;

import java.io.IOException;
import java.util.List;

/**
 * @author sullid (David Sullivan) on 29/11/2017
 * @project dp-elasticutils
 *
 * Simple class to expose index/search/delete request builders
 */
public class SimpleRestClient extends RestHighLevelClient {
    public SimpleRestClient(RestClientBuilder restClientBuilder) {
        super(restClientBuilder);
    }

    protected SimpleRestClient(RestClientBuilder restClientBuilder, List<NamedXContentRegistry.Entry> namedXContentEntries) {
        super(restClientBuilder, namedXContentEntries);
    }

    protected SimpleRestClient(RestClient restClient, CheckedConsumer<org.elasticsearch.client.RestClient, IOException> doClose, List<NamedXContentRegistry.Entry> namedXContentEntries) {
        super(restClient, doClose, namedXContentEntries);
    }

    public SimpleIndexRequestBuilder prepareIndex() {
        return new SimpleIndexRequestBuilder();
    }

    public SimpleSearchRequestBuilder prepareSearch(String... indices) {
        return new SimpleSearchRequestBuilder(indices);
    }

    public SimpleDeleteRequestBuilder prepareDelete() {
        return new SimpleDeleteRequestBuilder();
    }

    public SimpleDeleteRequestBuilder prepareDelete(String index, String type, String id) {
        return new SimpleDeleteRequestBuilder(index, type, id);
    }
}
