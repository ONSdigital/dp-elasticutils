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

Bulk indexing is asynchronous, and the client supports try with resources for auto-closing. The bulk processor however should be manually closed before client shutdown:

```java
private static BulkProcessorConfiguration getConfiguration() {
    BulkProcessorConfiguration bulkProcessorConfiguration = new BulkProcessorConfiguration(BulkProcessingOptions.builder()
            .setBulkActions(100)
            .setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB))
            .setFlushInterval(TimeValue.timeValueSeconds(5))
            .setConcurrentRequests(5)
            .setBackoffPolicy(
                    BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
            .build());
    return bulkProcessorConfiguration;
}

private static ElasticSearchClient<Movie> getClient(String hostName, String indexName) {
    SimpleRestClient client = ElasticSearchHelper.getRestClient(hostName, 9200);
    BulkProcessorConfiguration configuration = getConfiguration();
    return new OpenNlpSearchClient<Movie>(client, indexName, configuration, Movie.class);
}

// Client is shutdown automatically
try (ElasticSearchClient<Movie> searchClient = getClient("localhost", "movies")) {

    Iterable<Movie> it = Movie.finder().find();
    List<Movie> movies = new ArrayList<>();
    it.forEach(movies::add);

    searchClient.bulk(movies);
    // Await close on bulk insert
    searchClient.awaitClose(30, TimeUnit.SECONDS);
} catch (InterruptedException e) {
    e.printStackTrace();
} catch (Exception e) {
    e.printStackTrace();
}
```

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