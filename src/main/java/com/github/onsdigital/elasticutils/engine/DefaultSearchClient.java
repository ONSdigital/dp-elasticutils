package com.github.onsdigital.elasticutils.engine;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * @author sullid (David Sullivan) on 14/11/2017
 * @project elasticutils
 *
 * Interface to define default behaviour of a SearchClient
 */
public interface DefaultSearchClient<T> extends AutoCloseable {

    void index(T entity);

    void index(List<T> entities);

    void index(Stream<T> entities);

    void flush();

    boolean awaitClose(long timeout, TimeUnit unit) throws InterruptedException;

}
