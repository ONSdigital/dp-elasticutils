package com.github.onsdigital.elasticutils.models;

import com.github.onsdigital.elasticutils.TestHttpClient;
import com.github.onsdigital.elasticutils.client.generic.ElasticSearchClient;
import com.github.onsdigital.elasticutils.util.search.ObjectSearcher;

/**
 * @author sullid (David Sullivan) on 15/11/2017
 * @project dp-elasticutils
 *
 * Simple POJO to test Elasticsearch client
 */
public class GeoLocation {

    private String geoId;

    private double lat;

    private double lon;

    private GeoLocation() {}

    public GeoLocation(String geoId, double lat, double lon) {
        this.geoId = geoId;
        this.lat = lat;
        this.lon = lon;
    }

    public String getGeoId() {
        return geoId;
    }

    public double getLat() {
        return lat;
    }

    public double getLon() {
        return lon;
    }

    public static ObjectSearcher<GeoLocation> searcher(ElasticSearchClient<GeoLocation> searchClient) {
        return new ObjectSearcher<>(searchClient, TestHttpClient.ElasticIndex.TEST.getIndexName(), GeoLocation.class);
    }
}
