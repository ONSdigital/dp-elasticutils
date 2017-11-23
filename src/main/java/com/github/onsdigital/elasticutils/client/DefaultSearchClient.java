package com.github.onsdigital.elasticutils.client;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * @author sullid (David Sullivan) on 16/11/2017
 * @project dp-elasticutils
 */
public interface DefaultSearchClient<T> extends AutoCloseable {

    void index(T entity);

    void index(List<T> entities);

    void index(Stream<T> entities);

    void flush();

    boolean awaitClose(long timeout, TimeUnit unit) throws InterruptedException;

}
