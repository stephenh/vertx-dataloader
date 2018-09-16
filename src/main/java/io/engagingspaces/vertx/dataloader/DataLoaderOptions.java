/*
 * Copyright (c) 2016 The original author or authors
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 *
 *      The Eclipse Public License is available at
 *      http://www.eclipse.org/legal/epl-v10.html
 *
 *      The Apache License v2.0 is available at
 *      http://www.opensource.org/licenses/apache2.0.php
 *
 * You may elect to redistribute this code under either of these licenses.
 */

package io.engagingspaces.vertx.dataloader;

import java.util.Objects;
import java.util.Optional;

/**
 * Configuration options for {@link DataLoader} instances.
 *
 * @author <a href="https://github.com/aschrijver/">Arnold Schrijver</a>
 */
public class DataLoaderOptions {

    private boolean batchingEnabled;
    private boolean cachingEnabled;
    private CacheKey cacheKeyFunction;
    private CacheMap cacheMap;
    private Dispatcher dispatcher;
    private Object context;

    /**
     * Creates a new data loader options with default settings.
     */
    public DataLoaderOptions() {
        batchingEnabled = true;
        cachingEnabled = true;
    }

    public static DataLoaderOptions create() {
        return new DataLoaderOptions();
    }

    /**
     * Clones the provided data loader options.
     *
     * @param other the other options instance
     */
    public DataLoaderOptions(DataLoaderOptions other) {
        Objects.requireNonNull(other, "Other data loader options cannot be null");
        this.batchingEnabled = other.batchingEnabled;
        this.cachingEnabled = other.cachingEnabled;
        this.cacheKeyFunction = other.cacheKeyFunction;
        this.cacheMap = other.cacheMap;
        this.dispatcher = other.dispatcher;
        this.context = other.context;
    }

    /**
     * Option that determines whether to use batching (the default), or not.
     *
     * @return {@code true} when batching is enabled, {@code false} otherwise
     */
    public boolean batchingEnabled() {
        return batchingEnabled;
    }

    /**
     * Sets the option that determines whether batch loading is enabled.
     *
     * @param batchingEnabled {@code true} to enable batch loading, {@code false} otherwise
     * @return the data loader options for fluent coding
     */
    public DataLoaderOptions setBatchingEnabled(boolean batchingEnabled) {
        this.batchingEnabled = batchingEnabled;
        return this;
    }

    /**
     * Option that determines whether to use caching of futures (the default), or not.
     *
     * @return {@code true} when caching is enabled, {@code false} otherwise
     */
    public boolean cachingEnabled() {
        return cachingEnabled;
    }

    /**
     * Sets the option that determines whether caching is enabled.
     *
     * @param cachingEnabled {@code true} to enable caching, {@code false} otherwise
     * @return the data loader options for fluent coding
     */
    public DataLoaderOptions setCachingEnabled(boolean cachingEnabled) {
        this.cachingEnabled = cachingEnabled;
        return this;
    }

    /**
     * Gets an (optional) function to invoke for creation of the cache key, if caching is enabled.
     * <p>
     * If missing the cache key defaults to the {@code key} type parameter of the data loader of type {@code K}.
     *
     * @return an optional with the function, or empty optional
     */
    public Optional<CacheKey> cacheKeyFunction() {
        return Optional.ofNullable(cacheKeyFunction);
    }

    /**
     * Sets the function to use for creating the cache key, if caching is enabled.
     *
     * @param cacheKeyFunction the cache key function to use
     * @return the data loader options for fluent coding
     */
    public DataLoaderOptions setCacheKeyFunction(CacheKey cacheKeyFunction) {
        this.cacheKeyFunction = cacheKeyFunction;
        return this;
    }

    /**
     * Gets the (optional) cache map implementation that is used for caching, if caching is enabled.
     * <p>
     * If missing a standard {@link java.util.LinkedHashMap} will be used as the cache implementation.
     *
     * @return an optional with the cache map instance, or empty
     */
    public Optional<CacheMap> cacheMap() {
        return Optional.ofNullable(cacheMap);
    }

    /**
     * Sets the cache map implementation to use for caching, if caching is enabled.
     *
     * @param cacheMap the cache map instance
     * @return the data loader options for fluent coding
     */
    public DataLoaderOptions setCacheMap(CacheMap cacheMap) {
        this.cacheMap = cacheMap;
        return this;
    }

    /**
     * Gets the (optional) dispatcher that is used for automatic dispatching, if enabled.
     */
    public Optional<Dispatcher> dispatcher() {
        return Optional.ofNullable(dispatcher);
    }

    /**
     * Sets the dispather to use for automatic dispatching.
     *
     * @param dispatcher the dispatcher used for automatic dispatching
     * @return the data loader options for fluent coding
     */
    public DataLoaderOptions setDispatcher(Dispatcher dispatcher) {
        this.dispatcher = dispatcher;
        return this;
    }

    /**
     * @return The (optional) context that is used for automatic dispatching.
     */
    public Optional<Object> context() {
        return Optional.ofNullable(context);
    }

    /**
     * @param context The context that is used for automatic dispatching.
     * @return the data loader options for fluent coding
     */
    public DataLoaderOptions setContext(Object context) {
        this.context = context;
        return this;
    }

}
