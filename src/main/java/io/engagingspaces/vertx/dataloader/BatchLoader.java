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

import io.trane.future.Future;

import java.util.Collection;
import java.util.List;

/**
 * Function that is invoked for batch loading the list of data values indicated by the provided list of keys.
 *
 * @param <K> type parameter indicating the type of keys to use for data load requests.
 *
 * @author <a href="https://github.com/aschrijver/">Arnold Schrijver</a>
 */
@FunctionalInterface public interface BatchLoader<K, V> {

  /**
   * Batch load the provided keys and return a composite future of the result.
   *
   * @param keys the list of keys to load
   * @return the composite future
   */
  Future<List<V>> load(Collection<K> keys);
}
