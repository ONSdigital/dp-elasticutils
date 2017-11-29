package com.github.onsdigital.elasticutils.client.type;

/**
 * @author sullid (David Sullivan) on 29/11/2017
 * @project dp-elasticutils
 */
public enum DocumentType {

    DOCUMENT("document");

    private String type;

    DocumentType(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }
}
