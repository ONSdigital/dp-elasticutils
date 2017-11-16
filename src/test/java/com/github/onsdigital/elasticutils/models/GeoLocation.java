package com.github.onsdigital.elasticutils.models;

/**
 * @author sullid (David Sullivan) on 15/11/2017
 * @project dp-elasticutils
 */
public class GeoLocation {

    private double lat;

    private double lon;

    private GeoLocation() {}

    public GeoLocation(double lat, double lon) {
        this.lat = lat;
        this.lon = lon;
    }

    public double getLat() {
        return lat;
    }

    public double getLon() {
        return lon;
    }
}
