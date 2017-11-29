dp-elasticutils
================

### Description

A library for working with Elasticsearch 5.X and 6.X in Java. Implements both RESTful and TCP clients under a single easy to use API, with an option to pull a RESTful only version (restonly branch).

### Quickstart

See the test directory (src/test/java/) to see an example on how to get started.

Client connections via RESTful and TCP connections are straight forward:

```java

package com.github.onsdigital.elasticutils.models;

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
}


// RESTful HTTP client
SimpleRestClient client = ElasticSearchHelper.getRestClient(HOSTNAME, 9200);
BulkProcessorConfiguration configuration = ElasticSearchHelper.getDefaultBulkProcessorConfiguration();
String indexName = ElasticIndex.TEST.getIndexName();

ElasticSearchClient<GeoLocation> searchClient = new RestSearchClient<GeoLocation>(
        client, indexName, configuration, GeoLocation.class
);

// TCP client
TransportClient client = ElasticSearchHelper.getTransportClient(HOSTNAME, port.getPort(), settings);
BulkProcessorConfiguration configuration = ElasticSearchHelper.getDefaultBulkProcessorConfiguration();
String indexName = ElasticIndex.TEST.getIndexName();

ElasticSearchClient<GeoLocation> searchClient = new TransportSearchClient<GeoLocation>(
        client, indexName, configuration, GeoLocation.class
);
```

The ElasticSearchClient implements document indexing, search, and deletion aswell as deserialization of POJOs via the ElasticSearchResponse class.

### Maven dependency

The RESTful client supports Elasticsearch 5.X and 6.0.0, while the TCP client supports 6.0.0 ONLY. Docker will launch containers for Elasticsearch 5.5.0 and Elasticsearch 6.0.0 using the docker-compose.yml file provided.

To install, simply:

------
	docker-compose up
	mvn install
	docker-compose down

The maven dependency is:

```xml
    <dependency>
        <groupId>com.github.ONSdigital</groupId>
        <artifactId>dp-elasticutils</artifactId>
        <version>0.0.1-SNAPSHOT</version>
    </dependency>
```

### Testing

To run the tests, launch the docker container:

------
	docker-compose up
	mvn test
	docker-compose down