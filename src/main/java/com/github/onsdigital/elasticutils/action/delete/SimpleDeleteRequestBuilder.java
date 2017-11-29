package com.github.onsdigital.elasticutils.action.delete;

import com.github.onsdigital.elasticutils.action.SimpleActionRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.index.VersionType;

/**
 * @author sullid (David Sullivan) on 29/11/2017
 * @project dp-elasticutils
 */
public class SimpleDeleteRequestBuilder implements SimpleActionRequest<DeleteRequest> {

    private DeleteRequest request;

    public SimpleDeleteRequestBuilder() {
        this.request = new DeleteRequest();
    }

    public SimpleDeleteRequestBuilder(String index, String type, String id) {
        this.request = new DeleteRequest()
                .index(index)
                .type(type)
                .id(id);
    }

    @Override
    public DeleteRequest request() {
        return this.request;
    }

    /**
     * Sets the type of the document to delete.
     */
    public SimpleDeleteRequestBuilder setType(String type) {
        request.type(type);
        return this;
    }

    /**
     * Sets the id of the document to delete.
     */
    public SimpleDeleteRequestBuilder setId(String id) {
        request.id(id);
        return this;
    }

    /**
     * Sets the parent id of this document. Will simply set the routing to this value, as it is only
     * used for routing with delete requests.
     */
    public SimpleDeleteRequestBuilder setParent(String parent) {
        request.parent(parent);
        return this;
    }

    /**
     * Controls the shard routing of the delete request. Using this value to hash the shard
     * and not the id.
     */
    public SimpleDeleteRequestBuilder setRouting(String routing) {
        request.routing(routing);
        return this;
    }

    /**
     * Sets the version, which will cause the delete operation to only be performed if a matching
     * version exists and no changes happened on the doc since then.
     */
    public SimpleDeleteRequestBuilder setVersion(long version) {
        request.version(version);
        return this;
    }

    /**
     * Sets the type of versioning to use. Defaults to {@link VersionType#INTERNAL}.
     */
    public SimpleDeleteRequestBuilder setVersionType(VersionType versionType) {
        request.versionType(versionType);
        return this;
    }

}
