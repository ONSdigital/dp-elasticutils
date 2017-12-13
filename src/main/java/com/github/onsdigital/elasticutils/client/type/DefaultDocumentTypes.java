package com.github.onsdigital.elasticutils.client.type;

/**
 * @author sullid (David Sullivan) on 13/12/2017
 * @project dp-elasticutils
 */
public enum DefaultDocumentTypes implements DocumentType {

    DOCUMENT("document");

    private String type;

    DefaultDocumentTypes(String type) {
        this.type = type;
    }

    @Override
    public String getType() {
        return this.type;
    }
}
