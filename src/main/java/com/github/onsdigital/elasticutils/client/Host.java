package com.github.onsdigital.elasticutils.client;

/**
 * @author sullid (David Sullivan) on 01/12/2017
 * @project dp-elasticutils
 */
public enum Host {

    LOCALHOST("localhost");

    private String hostName;

    Host(String hostName) {
        this.hostName = hostName;
    }

    public String getHostName() {
        return hostName;
    }
}
