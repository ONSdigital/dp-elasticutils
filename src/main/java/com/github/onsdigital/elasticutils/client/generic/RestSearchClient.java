package com.github.onsdigital.elasticutils.client.generic;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.onsdigital.elasticutils.client.Host;
import com.github.onsdigital.elasticutils.client.bulk.configuration.BulkProcessorConfiguration;
import com.github.onsdigital.elasticutils.client.http.SimpleRestClient;
import com.github.onsdigital.elasticutils.client.type.DocumentType;
import com.github.onsdigital.elasticutils.util.ElasticSearchHelper;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.main.MainResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.common.http.HttpMethod;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;

/**
 * @author sullid (David Sullivan) on 29/11/2017
 * @project dp-elasticutils
 */
public class RestSearchClient<T> extends ElasticSearchClient<T> {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private SimpleRestClient client;
    private final BulkProcessor bulkProcessor;

    public RestSearchClient(SimpleRestClient client, final BulkProcessorConfiguration configuration) {
        this.client = client;
        this.bulkProcessor = configuration.build(this.client);
    }

    // INDEX //

    @Override
    public BulkProcessor getBulkProcessor() {
        return this.bulkProcessor;
    }

    @Override
    public IndexResponse index(IndexRequest request) throws IOException {
        IndexResponse response = this.client.index(request);
        return response;
    }

    // SEARCH //

    @Override
    public SearchResponse search(SearchRequest request) throws IOException {
        SearchResponse response = this.client.search(request);
        return response;
    }

    // DELETE //

    @Override
    public boolean dropIndex(String index) throws IOException {
        RestClient client = this.getLowLevelClient();

        String endpoint = endpoint(index);
        Response response = client.performRequest(HttpMethod.DELETE.method(), endpoint);
        return response.getStatusLine().getStatusCode() == HttpStatus.SC_OK;
    }

    // MISC //

    public MainResponse info() throws IOException {
        return this.client.info();
    }

    // LOW LEVEL CLIENT //

    protected RestClient getLowLevelClient() {
        return this.client.getLowLevelClient();
    }

    static String endpoint(String[] indices, String[] types, String endpoint) {
        return endpoint(String.join(",", indices), String.join(",", types), endpoint);
    }

    /**
     * Utility method to build request's endpoint.
     */
    static String endpoint(String... parts) {
        StringJoiner joiner = new StringJoiner("/", "/", "");
        for (String part : parts) {
            if (Strings.hasLength(part)) {
                joiner.add(part);
            }
        }
        return joiner.toString();
    }

    // ADMIN //

    @Override
    public boolean indexExists(String index) {
        RestClient client = this.getLowLevelClient();

        String endpoint = endpoint(index);
        try {
            Response response = client.performRequest(HttpMethod.HEAD.method(), endpoint);
            return response.getStatusLine().getStatusCode() == HttpStatus.SC_OK;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean createIndex(String index, DocumentType documentType, Settings settings, Map<String, Object> mapping) {
        RestClient client = this.getLowLevelClient();

        String endpoint = endpoint(index);
        Map<String, String> params = Collections.emptyMap();

        Map<String, Object> content = new HashMap<>();
        content.put("mappings", new HashMap<String, Object>() {{
            put(documentType.getType(), mapping);
        }});
        content.put("settings", settings.getAsMap());
        try {
            String jsonString = MAPPER.writeValueAsString(content);

            HttpEntity httpEntity = new NStringEntity(jsonString, ContentType.APPLICATION_JSON);
            Response response = client.performRequest(HttpMethod.PUT.method(), endpoint, params, httpEntity);
            return response.getStatusLine().getStatusCode() == HttpStatus.SC_OK;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public void shutdown() throws IOException {
        this.client.close();
    }

    public static RestSearchClient getLocalClient() {
        BulkProcessorConfiguration configuration = ElasticSearchHelper.getDefaultBulkProcessorConfiguration();
        return getLocalClient(configuration);
    }

    public static RestSearchClient getLocalClient(BulkProcessorConfiguration configuration) {
        return new RestSearchClient<>(ElasticSearchHelper.getRestClient(Host.LOCALHOST), configuration);
    }

    public static void main(String[] args) {
        String index = "test";

        try (RestSearchClient searchClient = RestSearchClient.getLocalClient()) {
            boolean indexExists = searchClient.indexExists(index);
            System.out.println("Index exists: " + indexExists);

            if (indexExists) {
                searchClient.dropIndex(index);
            }

            Settings settings = ElasticSearchHelper.loadSettingsFromFile("/search/", "index-config.yml");
            Map<String, Object> mapping = ElasticSearchHelper.loadMappingFromFile("/search", "default-document-mapping.json");
            System.out.println(mapping);

            System.out.println(searchClient.createIndex(index, DocType.DOCUMENT, settings, mapping));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    enum DocType implements DocumentType {
        DOCUMENT("document");

        private String type;

        DocType(String type) {
            this.type = type;
        }

        @Override
        public String getType() {
            return type;
        }
    }
}
