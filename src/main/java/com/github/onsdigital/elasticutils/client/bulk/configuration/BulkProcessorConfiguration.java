// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package com.github.onsdigital.elasticutils.client.bulk.configuration;

import com.github.onsdigital.elasticutils.client.bulk.listener.LoggingBulkProcessorListener;
import com.github.onsdigital.elasticutils.client.bulk.options.BulkProcessingOptions;
import com.github.onsdigital.elasticutils.client.bulk.options.BulkProcessingOptionsBuilder;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

public class BulkProcessorConfiguration {

    private BulkProcessingOptions options = new BulkProcessingOptionsBuilder().build();
    private BulkProcessor.Listener listener = new LoggingBulkProcessorListener();

    public BulkProcessorConfiguration(BulkProcessingOptions options)
    {
        this(options, new LoggingBulkProcessorListener());
    }

    public BulkProcessorConfiguration(BulkProcessingOptions options, BulkProcessor.Listener listener) {
        this.options = options;
        this.listener = listener;
    }

    public BulkProcessingOptions getBulkProcessingOptions() {
        return options;
    }

    public BulkProcessor.Listener getBulkProcessorListener() {
        return listener;
    }

    public BulkProcessor build(final Client client) {
        return BulkProcessor.builder(client, listener)
//                .setName(options.getName())
                .setConcurrentRequests(options.getConcurrentRequests())
                .setBulkActions(options.getBulkActions())
                .setBulkSize(options.getBulkSize())
                .setFlushInterval(options.getFlushInterval())
                .setBackoffPolicy(options.getBackoffPolicy())
                .build();
    }

    public BulkProcessor build(final RestHighLevelClient client) {
        Settings defaultSettings = Settings.builder().put("node.name", "NodeScope").build();
        return build(client, defaultSettings);
    }

    public BulkProcessor build(final RestHighLevelClient client, Settings settings) {
        ThreadPool threadPool = new ThreadPool(settings);

        LoggingBulkProcessorListener listener = new LoggingBulkProcessorListener();
        BulkProcessor.Builder builder = new BulkProcessor.Builder(client::bulkAsync, listener, threadPool)
                .setConcurrentRequests(options.getConcurrentRequests())
                .setBulkActions(options.getBulkActions())
                .setBulkSize(options.getBulkSize())
                .setBackoffPolicy(options.getBackoffPolicy());

        return builder.build();
    }
}