package com.github.onsdigital.elasticutils.util;

import com.github.onsdigital.elasticutils.client.bulk.configuration.BulkProcessorConfiguration;
import com.github.onsdigital.elasticutils.client.bulk.options.BulkProcessingOptions;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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

    private static Map<Host, RestHighLevelClient> httpConnectionMap;
    private static Map<Host, TransportClient> tcpConnectionMap;

    static {
        httpConnectionMap = new ConcurrentHashMap<>();
        tcpConnectionMap = new ConcurrentHashMap<>();
    }

    // HTTP

    public static RestHighLevelClient getRestClient(String hostName) {
        return getRestClient(hostName, DEFAULT_HTTP_PORT);
    }

    public static RestHighLevelClient getRestClient(String hostName, int http_port) {

        Host host = new Host(hostName, new int[] {http_port});

        if(!httpConnectionMap.containsKey(host)) {

            LOGGER.info("Attempting to make HTTP connection to ES database: {} {}", hostName, http_port);

            // Set some basic headers for all requests
            BasicHeader[] headers = { new BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json") };

            RestHighLevelClient client = new RestHighLevelClient(
                    RestClient.builder(
                            new HttpHost(hostName, http_port, Scheme.HTTP.getScheme())
                    ).setDefaultHeaders(headers)
            );

            httpConnectionMap.put(host, client);

            LOGGER.info("Successfully made HTTP connection to ES database: {} {}", hostName, http_port);

        }

        return httpConnectionMap.get(host);
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

        Host host = new Host(hostName, transport_ports);

        if(!tcpConnectionMap.containsKey(host)) {

            if (LOGGER.isInfoEnabled())
                LOGGER.info(String.format("Attempting to make TCP connection to ES db %s", hostName));

            TransportClient client = new PreBuiltTransportClient(settings);
            for (int port : transport_ports) {
                client.addTransportAddress(new TransportAddress(InetAddress.getByName(hostName), port));
            }

            tcpConnectionMap.put(host, client);

            if (LOGGER.isInfoEnabled())
                LOGGER.info(String.format("Successfully made connection to ES db %s", hostName));

        }

        return tcpConnectionMap.get(host);
    }

    public static TransportClient getXpackTransportClient(String hostName, Settings settings) throws UnknownHostException {
        return getXpackTransportClient(hostName, DEFAULT_TCP_PORT, DEFAULT_XPACK_TCP_PORT, settings);
    }

    public static TransportClient getXpackTransportClient(String hostName, int transport_port, int xpack_transport_port, Settings settings) throws UnknownHostException {
        // Don't cache an x-pack client, force re-authentication
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
        return getDefaultBulkProcessorConfiguration(100);
    }

    public static BulkProcessorConfiguration getDefaultBulkProcessorConfiguration(int numBulkActions) {
        BulkProcessorConfiguration bulkProcessorConfiguration = new BulkProcessorConfiguration(BulkProcessingOptions.builder()
                .setBulkActions(numBulkActions)
                .build());
        return bulkProcessorConfiguration;
    }

    public enum ClientType {
        TCP("TCP"),
        REST("REST");

        private String clientType;

        ClientType(String clientType) {
            this.clientType = clientType;
        }

        public String getClientType() {
            return clientType;
        }
    }

    public enum Scheme {
        HTTP("http");

        private String scheme;

        Scheme(String scheme) {
            this.scheme = scheme;
        }

        public String getScheme() {
            return scheme;
        }
    }

}

final class Host {

    private final String hostName;
    private final int[] ports;

    public Host(String hostName, int[] ports) {
        this.hostName = hostName;
        this.ports = ports;
    }

    public final String getHostName() {
        return hostName;
    }

    public final int[] getPorts() {
        return ports;
    }

    @Override
    public boolean equals(Object otherObject) {
        if (otherObject instanceof Host) {
            Host otherHost = (Host) otherObject;

            return (this.getHostName().equals(otherHost.getHostName()) && this.getPorts() == otherHost.getPorts());
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return (this.getHostName() + ":" + this.getPorts()).hashCode();
    }
}
