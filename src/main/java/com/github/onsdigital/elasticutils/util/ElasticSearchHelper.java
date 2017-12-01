package com.github.onsdigital.elasticutils.util;

import com.github.onsdigital.elasticutils.client.http.SimpleRestClient;
import com.github.onsdigital.elasticutils.client.bulk.configuration.BulkProcessorConfiguration;
import com.github.onsdigital.elasticutils.client.bulk.options.BulkProcessingOptions;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @author sullid (David Sullivan) on 14/11/2017
 * @project elasticutils
 *
 * Helper class to get a valid Elastic Search client.
 * Implements several methods for getting a Client with/without supplying ports/settings
 */
public class ElasticSearchHelper {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticSearchHelper.class);

    public static final int DEFAULT_HTTP_PORT = 9200;

    public static final int DEFAULT_TCP_PORT = 9300;
    public static final int DEFAULT_XPACK_TCP_PORT = 9301;

    // HTTP

    public static SimpleRestClient getRestClient(String hostName) {
        return getRestClient(hostName, DEFAULT_HTTP_PORT);
    }

    public static SimpleRestClient getRestClient(String hostName, int http_port) {

        LOGGER.info("Attempting to make HTTP connection to ES database: {} {}", hostName, http_port);

        // Set some basic headers for all requests
        BasicHeader[] headers = { new BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json") };

        RestClientBuilder builder = RestClient.builder(new HttpHost(hostName, http_port))
                        .setDefaultHeaders(headers);

        SimpleRestClient client = new SimpleRestClient(builder);

        LOGGER.info("Successfully made HTTP connection to ES database: {} {}", hostName, http_port);
        return client;
    }

    // TCP

    public static TransportClient getTransportClient(String hostName) throws UnknownHostException {
        return getTransportClient(hostName, new int[] {DEFAULT_TCP_PORT});
    }

    public static TransportClient getTransportClient(String hostName, int transport_port) throws UnknownHostException {
        return getTransportClient(hostName, new int[] {transport_port});
    }

    public static TransportClient getTransportClient(String hostName, int transport_port, Settings settings) throws UnknownHostException {
        return getTransportClient(hostName, new int[] {transport_port}, settings);
    }

    public static TransportClient getTransportClient(String hostName, int[] transport_ports) throws UnknownHostException {
        Settings defaultSettings = Settings.EMPTY;
        return getTransportClient(hostName, transport_ports, defaultSettings);
    }

    public static TransportClient getTransportClient(String hostName, int[] transport_ports, Settings settings) throws UnknownHostException {

        if (LOGGER.isInfoEnabled())
            LOGGER.info(String.format("Attempting to make TCP connection to ES db %s", hostName));

        TransportClient client = new PreBuiltTransportClient(settings);
        for (int port : transport_ports) {
            client.addTransportAddress(new TransportAddress(InetAddress.getByName(hostName), port));
        }

        if (LOGGER.isInfoEnabled())
            LOGGER.info(String.format("Successfully made connection to ES db %s", hostName));

        return client;
    }

    public static TransportClient getXpackTransportClient(String hostName, Settings settings) throws UnknownHostException {
        return getXpackTransportClient(hostName, DEFAULT_TCP_PORT, DEFAULT_XPACK_TCP_PORT, settings);
    }

    public static TransportClient getXpackTransportClient(String hostName, int transport_port, int xpack_transport_port, Settings settings) throws UnknownHostException {
        if(LOGGER.isInfoEnabled()) LOGGER.info(String.format("Attempting to make connection to X-pack enabled ES db %s", hostName));

        TransportClient transportClient = new PreBuiltXPackTransportClient(settings)
                .addTransportAddress(new TransportAddress(InetAddress.getByName(hostName), transport_port))
                .addTransportAddress(new TransportAddress(InetAddress.getByName(hostName), xpack_transport_port));

        if(LOGGER.isInfoEnabled()) LOGGER.info(String.format("Successfully made connection to X-pack enabled ES db %s", hostName));

        return transportClient;
    }

    public static BulkProcessorConfiguration getDefaultBulkProcessorConfiguration() {
        /*
        Return the default BulkProcessorConfiguration with a maximum of 100 bulk actions
         */
        return getDefaultBulkProcessorConfiguration(10000);
    }

    public static BulkProcessorConfiguration getDefaultBulkProcessorConfiguration(int numBulkActions) {
        return getDefaultBulkProcessorConfiguration(numBulkActions, 1);
    }

    public static BulkProcessorConfiguration getDefaultBulkProcessorConfiguration(int numBulkActions, int numConcurrentRequests) {
        BulkProcessorConfiguration bulkProcessorConfiguration = new BulkProcessorConfiguration(BulkProcessingOptions.builder()
                .setBulkActions(numBulkActions)
                .setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB))
                .setFlushInterval(TimeValue.timeValueSeconds(5))
                .setConcurrentRequests(numConcurrentRequests)
                .setBackoffPolicy(
                        BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
                .build());
        return bulkProcessorConfiguration;
    }

}
