package com.github.onsdigital.elasticutils.action.index;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.onsdigital.elasticutils.action.SimpleActionRequest;
import com.github.onsdigital.elasticutils.util.JsonUtils;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;

import java.util.Optional;

/**
 * @author sullid (David Sullivan) on 29/11/2017
 * @project dp-elasticutils
 *
 * Simple class to build IndexRequests
 */
public class SimpleIndexRequestBuilder implements SimpleActionRequest<IndexRequest> {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private IndexRequest request;

    public SimpleIndexRequestBuilder() {
        request = new IndexRequest();
    }

    public SimpleIndexRequestBuilder(String index) {
        request = new IndexRequest(index);
    }

    public SimpleIndexRequestBuilder(String index, String type) {
        request = new IndexRequest(index).type(type);
    }

    @Override
    public IndexRequest request() {
        return this.request;
    }

    public SimpleIndexRequestBuilder setIndex(String index) {
        request.index(index);
        return this;
    }

    /**
     * Sets the type to index the document to.
     */
    public SimpleIndexRequestBuilder setType(String type) {
        request.type(type);
        return this;
    }

    /**
     * Sets the id to index the document under. Optional, and if not set, one will be automatically
     * generated.
     */
    public SimpleIndexRequestBuilder setId(String id) {
        request.id(id);
        return this;
    }

    /**
     * Controls the shard routing of the request. Using this value to hash the shard
     * and not the id.
     */
    public SimpleIndexRequestBuilder setRouting(String routing) {
        request.routing(routing);
        return this;
    }

    /**
     * Sets the parent id of this document. If routing is not set, automatically set it as the
     * routing as well.
     */
    public SimpleIndexRequestBuilder setParent(String parent) {
        request.parent(parent);
        return this;
    }

    public SimpleIndexRequestBuilder setSource(Object source) {
        return this.setSource(source, XContentType.JSON);
    }

    public SimpleIndexRequestBuilder setSource(Object source, XContentType xContentType) {
        byte[] messageBytes = JsonUtils.convertJsonToBytes(source).get();
        return this.setSource(messageBytes, xContentType);
    }

    public SimpleIndexRequestBuilder setSource(byte[] source) {
        return this.setSource(source, XContentType.JSON);
    }

    public SimpleIndexRequestBuilder setSource(byte[] source, XContentType xContentType) {
        request.source(source, xContentType);
        return this;
    }

    /**
     * Sets the type of operation to perform.
     */
    public SimpleIndexRequestBuilder setOpType(DocWriteRequest.OpType opType) {
        request.opType(opType);
        return this;
    }

    /**
     * Set to <tt>true</tt> to force this index to use {@link org.elasticsearch.action.index.IndexRequest.OpType#CREATE}.
     */
    public SimpleIndexRequestBuilder setCreate(boolean create) {
        request.create(create);
        return this;
    }

    /**
     * Sets the version, which will cause the index operation to only be performed if a matching
     * version exists and no changes happened on the doc since then.
     */
    public SimpleIndexRequestBuilder setVersion(long version) {
        request.version(version);
        return this;
    }

    /**
     * Sets the versioning type. Defaults to {@link VersionType#INTERNAL}.
     */
    public SimpleIndexRequestBuilder setVersionType(VersionType versionType) {
        request.versionType(versionType);
        return this;
    }

    /**
     * Sets the ingest pipeline to be executed before indexing the document
     */
    public SimpleIndexRequestBuilder setPipeline(String pipeline) {
        request.setPipeline(pipeline);
        return this;
    }

}
