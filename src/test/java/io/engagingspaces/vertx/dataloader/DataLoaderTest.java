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
import io.trane.future.Promise;
import io.trane.future.Responder;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.Tuple3;
import io.vavr.Tuple4;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.*;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link DataLoader}.
 * <p>
 * The tests are a port of the existing tests in
 * the <a href="https://github.com/facebook/dataloader">facebook/dataloader</a> project.
 * <p>
 * Acknowledgments go to <a href="https://github.com/leebyron">Lee Byron</a> for providing excellent coverage.
 *
 * @author <a href="https://github.com/aschrijver/">Arnold Schrijver</a>
 */
@RunWith(VertxUnitRunner.class) public class DataLoaderTest {

  @Rule public RunTestOnContext rule = new RunTestOnContext();

  DataLoader<Integer, Integer> identityLoader;

  @Before public void setUp() {
    identityLoader = idLoader(new DataLoaderOptions(), new ArrayList<>());
  }

  @Test public void should_Build_a_really_really_simple_data_loader(TestContext t) {
    DataLoader<Integer, Integer> identityLoader = new DataLoader<>(keys -> Future.value(new ArrayList<>(keys)));

    Future<Integer> future1 = identityLoader.load(1);
    future1.onSuccess(i -> {
      assertThat(i, equalTo(1));
    }).respond(done(t));
    identityLoader.dispatch();
  }

  @Test public void should_Support_loading_multiple_keys_in_one_call(TestContext t) {
    DataLoader<Integer, Integer> identityLoader = new DataLoader<>(keys -> Future.value(new ArrayList<>(keys)));

    Future<List<Integer>> futureAll = identityLoader.loadMany(asList(1, 2));
    futureAll.onSuccess(r -> {
      assertThat(r.size(), is(2));
      assertThat(r, equalTo(asList(1, 2)));
    }).respond(done(t));
    identityLoader.dispatch();
  }

  @Test public void should_Resolve_to_empty_list_when_no_keys_supplied(TestContext t) {
    Future<List<Integer>> futureEmpty = identityLoader.loadMany(Collections.emptyList());
    futureEmpty.onSuccess(r -> {
      assertThat(r.size(), is(0));
      assertThat(r, empty());
    }).respond(done(t));
    identityLoader.dispatch();
  }

  @Test public void should_Batch_multiple_requests(TestContext t) {
    ArrayList<Collection> loadCalls = new ArrayList<>();
    DataLoader<Integer, Integer> identityLoader = idLoader(new DataLoaderOptions(), loadCalls);

    Future<Integer> future1 = identityLoader.load(1);
    Future<Integer> future2 = identityLoader.load(2);
    identityLoader.dispatch();

    join(future1, future2).onSuccess(result -> {
      assertThat(result._1, equalTo(1));
      assertThat(result._2, equalTo(2));
      assertThat(loadCalls, equalTo(Collections.singletonList(asList(1, 2))));
    }).respond(done(t));
  }

  private static <T1, T2> Future<Tuple2<T1, T2>> join(Future<T1> f1, Future<T2> f2) {
    return Future.collect(asList((Future<Object>) f1, (Future<Object>) f2)).map(list -> Tuple.of((T1) list.get(0), (T2) list.get(1)));
  }

  private static <T1, T2, T3> Future<Tuple3<T1, T2, T3>> join(Future<T1> f1, Future<T2> f2, Future<T3> f3) {
    return Future.collect(asList((Future<Object>) f1, (Future<Object>) f2, (Future<Object>) f3))
      .map(list -> Tuple.of((T1) list.get(0), (T2) list.get(1), (T3) list.get(2)));
  }

  private static <T1, T2, T3, T4> Future<Tuple4<T1, T2, T3, T4>> join(Future<T1> f1, Future<T2> f2, Future<T3> f3, Future<T4> f4) {
    return Future.collect(asList((Future<Object>) f1, (Future<Object>) f2, (Future<Object>) f3, (Future<Object>) f4))
      .map(list -> Tuple.of((T1) list.get(0), (T2) list.get(1), (T3) list.get(2), (T4) list.get(3)));
  }

  @Test public void should_Coalesce_identical_requests(TestContext t) {
    ArrayList<Collection> loadCalls = new ArrayList<>();
    DataLoader<Integer, Integer> identityLoader = idLoader(new DataLoaderOptions(), loadCalls);

    Future<Integer> future1a = identityLoader.load(1);
    Future<Integer> future1b = identityLoader.load(1);
    assertThat(future1a, sameInstance(future1b));
    identityLoader.dispatch();

    join(future1a, future1b).onSuccess(k -> {
      assertThat(k._1, equalTo(1));
      assertThat(k._2, equalTo(1));
      assertThat(loadCalls, equalTo(Collections.singletonList(Collections.singletonList(1))));
    }).respond(done(t));
  }

  @Test public void should_Cache_repeated_requests(TestContext t) {
    ArrayList<Collection> loadCalls = new ArrayList<>();
    DataLoader<String, String> identityLoader = idLoader(new DataLoaderOptions(), loadCalls);

    Future<String> future1 = identityLoader.load("A");
    Future<String> future2 = identityLoader.load("B");
    identityLoader.dispatch();

    join(future1, future2).flatMap(result -> {
      assertThat(result._1, equalTo("A"));
      assertThat(result._2, equalTo("B"));
      assertThat(loadCalls, equalTo(Collections.singletonList(asList("A", "B"))));

      Future<String> future1a = identityLoader.load("A");
      Future<String> future3 = identityLoader.load("C");
      identityLoader.dispatch();

      return join(future1a, future3);
    }).flatMap(result -> {
      assertThat(result._1, equalTo("A"));
      assertThat(result._2, equalTo("C"));
      assertThat(loadCalls, equalTo(asList(asList("A", "B"), Collections.singletonList("C"))));

      Future<String> future1b = identityLoader.load("A");
      Future<String> future2a = identityLoader.load("B");
      Future<String> future3a = identityLoader.load("C");
      identityLoader.dispatch();

      return join(future1b, future2a, future3a);
    }).onSuccess(result -> {
      assertThat(result._1, equalTo("A"));
      assertThat(result._2, equalTo("B"));
      assertThat(result._3, equalTo("C"));
      assertThat(loadCalls, equalTo(asList(asList("A", "B"), Collections.singletonList("C"))));
    }).respond(done(t));
  }

  @Test public void should_Not_redispatch_previous_load(TestContext t) {
    ArrayList<Collection> loadCalls = new ArrayList<>();
    DataLoader<String, String> identityLoader = idLoader(new DataLoaderOptions(), loadCalls);

    Future<String> future1 = identityLoader.load("A");
    identityLoader.dispatch();

    Future<String> future2 = identityLoader.load("B");
    identityLoader.dispatch();

    join(future1, future2).onSuccess(result -> {
      assertThat(result._1, equalTo("A"));
      assertThat(result._2, equalTo("B"));
      assertThat(loadCalls, equalTo(asList(asList("A"), asList("B"))));
    }).respond(done(t));
  }

  @Test public void should_Cache_on_redispatch(TestContext t) {
    ArrayList<Collection> loadCalls = new ArrayList<>();
    DataLoader<String, String> identityLoader = idLoader(new DataLoaderOptions(), loadCalls);

    Future<String> future1 = identityLoader.load("A");
    identityLoader.dispatch();

    Future<List<String>> future2 = identityLoader.loadMany(asList("A", "B"));
    identityLoader.dispatch();

    join(future1, future2).onSuccess(result -> {
      assertThat(result._1, equalTo("A"));
      assertThat(result._2, equalTo(asList("A", "B")));
      assertThat(loadCalls, equalTo(asList(asList("A"), asList("B"))));
    }).respond(done(t));
  }

  @Test public void should_Clear_single_value_in_loader(TestContext t) {
    ArrayList<Collection> loadCalls = new ArrayList<>();
    DataLoader<String, String> identityLoader = idLoader(new DataLoaderOptions(), loadCalls);

    Future<String> future1 = identityLoader.load("A");
    Future<String> future2 = identityLoader.load("B");
    identityLoader.dispatch();

    join(future1, future2).flatMap(result -> {
      assertThat(result._1, equalTo("A"));
      assertThat(result._2, equalTo("B"));
      assertThat(loadCalls, equalTo(Collections.singletonList(asList("A", "B"))));

      identityLoader.clear("A");

      Future<String> future1a = identityLoader.load("A");
      Future<String> future2a = identityLoader.load("B");
      identityLoader.dispatch();

      return join(future1a, future2a);
    }).onSuccess(result -> {
      assertThat(result._1, equalTo("A"));
      assertThat(result._2, equalTo("B"));
      assertThat(loadCalls, equalTo(asList(asList("A", "B"), Collections.singletonList("A"))));
    }).respond(done(t));
  }

  @Test public void should_Clear_all_values_in_loader(TestContext t) {
    ArrayList<Collection> loadCalls = new ArrayList<>();
    DataLoader<String, String> identityLoader = idLoader(new DataLoaderOptions(), loadCalls);

    Future<String> future1 = identityLoader.load("A");
    Future<String> future2 = identityLoader.load("B");
    identityLoader.dispatch();

    join(future1, future2).flatMap(result -> {
      assertThat(result._1, equalTo("A"));
      assertThat(result._2, equalTo("B"));
      assertThat(loadCalls, equalTo(Collections.singletonList(asList("A", "B"))));

      identityLoader.clearAll();

      Future<String> future1a = identityLoader.load("A");
      Future<String> future2a = identityLoader.load("B");
      identityLoader.dispatch();

      return join(future1a, future2a);
    }).onSuccess(result -> {
      assertThat(result._1, equalTo("A"));
      assertThat(result._2, equalTo("B"));
      assertThat(loadCalls, equalTo(asList(asList("A", "B"), asList("A", "B"))));
    }).respond(done(t));
  }

  @Test public void should_Allow_priming_the_cache(TestContext t) {
    ArrayList<Collection> loadCalls = new ArrayList<>();
    DataLoader<String, String> identityLoader = idLoader(new DataLoaderOptions(), loadCalls);

    identityLoader.prime("A", "A");

    Future<String> future1 = identityLoader.load("A");
    Future<String> future2 = identityLoader.load("B");
    identityLoader.dispatch();

    join(future1, future2).onSuccess(result -> {
      assertThat(result._1, equalTo("A"));
      assertThat(result._2, equalTo("B"));
      assertThat(loadCalls, equalTo(Collections.singletonList(Collections.singletonList("B"))));
    }).respond(done(t));
  }

  @Test public void should_Not_prime_keys_that_already_exist(TestContext t) {
    ArrayList<Collection> loadCalls = new ArrayList<>();
    DataLoader<String, String> identityLoader = idLoader(new DataLoaderOptions(), loadCalls);

    identityLoader.prime("A", "X");

    Future<String> future1 = identityLoader.load("A");
    Future<String> future2 = identityLoader.load("B");
    identityLoader.dispatch();

    join(future1, future2).flatMap(result -> {
      assertThat(result._1, equalTo("X"));
      assertThat(result._2, equalTo("B"));

      identityLoader.prime("A", "Y");
      identityLoader.prime("B", "Y");

      Future<String> future1a = identityLoader.load("A");
      Future<String> future2a = identityLoader.load("B");
      identityLoader.dispatch();

      return join(future1a, future2a);
    }).onSuccess(result -> {
      assertThat(result._1, equalTo("X"));
      assertThat(result._2, equalTo("B"));
      assertThat(loadCalls, equalTo(Collections.singletonList(Collections.singletonList("B"))));
    }).respond(done(t));
  }

  @Test public void should_Allow_to_forcefully_prime_the_cache(TestContext t) {
    ArrayList<Collection> loadCalls = new ArrayList<>();
    DataLoader<String, String> identityLoader = idLoader(new DataLoaderOptions(), loadCalls);

    identityLoader.prime("A", "X");

    Future<String> future1 = identityLoader.load("A");
    Future<String> future2 = identityLoader.load("B");
    identityLoader.dispatch();

    join(future1, future2).flatMap(results -> {
      assertThat(results._1, equalTo("X"));
      assertThat(results._2, equalTo("B"));

      identityLoader.clear("A").prime("A", "Y");
      identityLoader.clear("B").prime("B", "Y");

      Future<String> future1a = identityLoader.load("A");
      Future<String> future2a = identityLoader.load("B");
      identityLoader.dispatch();

      return join(future1a, future2a);
    }).onSuccess(result -> {
      assertThat(result._1, equalTo("Y"));
      assertThat(result._2, equalTo("Y"));
      assertThat(loadCalls, equalTo(Collections.singletonList(Collections.singletonList("B"))));
    }).respond(done(t));
  }

  @Test public void should_Cache_failed_fetches(TestContext t) {
    ArrayList<Collection> loadCalls = new ArrayList<>();
    DataLoader<Integer, Integer> errorLoader = idLoaderAllErrors(new DataLoaderOptions(), loadCalls);

    Future<Integer> future1 = errorLoader.load(1);
    errorLoader.dispatch();

    future1.rescue(cause -> {
      assertThat(cause, instanceOf(IllegalStateException.class));

      Future<Integer> future2 = errorLoader.load(1);
      errorLoader.dispatch();

      return future2;
    }).onFailure(cause -> {
      assertThat(cause, instanceOf(IllegalStateException.class));
      assertThat(loadCalls, equalTo(Collections.singletonList(Collections.singletonList(1))));
    }).respond(failed(t));
  }

  @Test public void should_Handle_priming_the_cache_with_an_error(TestContext t) {
    ArrayList<Collection> loadCalls = new ArrayList<>();
    DataLoader<Integer, Integer> identityLoader = idLoader(new DataLoaderOptions(), loadCalls);

    identityLoader.prime(1, new IllegalStateException("Error"));

    Future<Integer> future1 = identityLoader.load(1);
    identityLoader.dispatch();

    future1.onFailure(cause -> {
      assertThat(cause, instanceOf(IllegalStateException.class));
      assertThat(loadCalls, equalTo(Collections.emptyList()));
    }).respond(failed(t));
  }

  @Test public void should_Clear_values_from_cache_after_errors(TestContext t) {
    ArrayList<Collection> loadCalls = new ArrayList<>();
    DataLoader<Integer, Integer> errorLoader = idLoaderAllErrors(new DataLoaderOptions(), loadCalls);

    Future<Integer> future1 = errorLoader.load(1);
    future1.onFailure(cause -> {
      // Presumably determine if this error is transient, and only clear the cache in that case.
      errorLoader.clear(1);
      assertThat(cause, instanceOf(IllegalStateException.class));

      Future<Integer> future2 = errorLoader.load(1);
      future2.onFailure(cause2 -> {
        // Again, only do this if you can determine the error is transient.
        errorLoader.clear(1);
        assertThat(cause2, instanceOf(IllegalStateException.class));
        assertThat(loadCalls, equalTo(asList(Collections.singletonList(1), Collections.singletonList(1))));
        t.async().complete();
      });
      errorLoader.dispatch();
    });
    errorLoader.dispatch();
  }

  @Test public void should_Propagate_error_to_all_loads(TestContext t) {
    ArrayList<Collection> loadCalls = new ArrayList<>();
    DataLoader<Integer, Integer> errorLoader = idLoaderAllErrors(new DataLoaderOptions(), loadCalls);

    Future<Integer> future1 = errorLoader.load(1);
    future1.onFailure(cause -> {
      assertThat(cause, instanceOf(IllegalStateException.class));
      assertThat(cause.getMessage(), equalTo("Error"));
    });

    Future<Integer> future2 = errorLoader.load(2);
    future2.onFailure(cause -> {
      assertThat(cause, instanceOf(IllegalStateException.class));
      assertThat(cause.getMessage(), equalTo("Error"));
    });

    errorLoader.dispatch();

    join(future1, future2).respond(failed(t));
  }

  // Accept any kind of key.

  @Test public void should_Accept_objects_as_keys(TestContext t) {
    ArrayList<Collection> loadCalls = new ArrayList<>();
    DataLoader<Object, Object> identityLoader = idLoader(new DataLoaderOptions(), loadCalls);

    Object keyA = new Object();
    Object keyB = new Object();

    // Fetches as expected

    identityLoader.load(keyA);
    identityLoader.load(keyB);

    identityLoader.dispatch().flatMap(results -> {
      assertThat(results.get(0), equalTo(keyA));
      assertThat(results.get(1), equalTo(keyB));

      assertThat(loadCalls.size(), equalTo(1));
      assertThat(loadCalls.get(0).size(), equalTo(2));
      assertThat(loadCalls.get(0).toArray()[0], equalTo(keyA));
      assertThat(loadCalls.get(0).toArray()[1], equalTo(keyB));

      // Caching
      identityLoader.clear(keyA);
      //noinspection SuspiciousMethodCalls
      loadCalls.remove(keyA);

      identityLoader.load(keyA);
      identityLoader.load(keyB);

      return identityLoader.dispatch().onSuccess(results2 -> {
        assertThat(results2.get(0), equalTo(keyA));
        assertThat(identityLoader.getCacheKey(keyB), equalTo(keyB));

        assertThat(loadCalls.size(), equalTo(2));
        assertThat(loadCalls.get(1).size(), equalTo(1));
        assertThat(loadCalls.get(1).toArray()[0], equalTo(keyA));
      });
    }).respond(done(t));
  }

  // Accepts options

  @Test public void should_Disable_caching(TestContext t) {
    ArrayList<Collection> loadCalls = new ArrayList<>();
    DataLoader<String, String> identityLoader = idLoader(DataLoaderOptions.create().setCachingEnabled(false), loadCalls);

    Future<String> future1 = identityLoader.load("A");
    Future<String> future2 = identityLoader.load("B");
    identityLoader.dispatch();

    join(future1, future2).flatMap(result -> {
      assertThat(result._1, equalTo("A"));
      assertThat(result._2, equalTo("B"));
      assertThat(loadCalls, equalTo(Collections.singletonList(asList("A", "B"))));

      Future<String> future1a = identityLoader.load("A");
      Future<String> future3 = identityLoader.load("C");
      identityLoader.dispatch();

      return join(future1a, future3);
    }).flatMap(result -> {
      assertThat(result._1, equalTo("A"));
      assertThat(result._2, equalTo("C"));
      assertThat(loadCalls, equalTo(asList(asList("A", "B"), asList("A", "C"))));

      Future<String> future1b = identityLoader.load("A");
      Future<String> future2a = identityLoader.load("B");
      Future<String> future3a = identityLoader.load("C");
      identityLoader.dispatch();

      return join(future1b, future2a, future3a);
    }).onSuccess(result -> {
      assertThat(result._1, equalTo("A"));
      assertThat(result._2, equalTo("B"));
      assertThat(result._3, equalTo("C"));
      assertThat(loadCalls, equalTo(asList(asList("A", "B"), asList("A", "C"), asList("A", "B", "C"))));
    }).respond(done(t));
  }

  // Accepts object key in custom cacheKey function

  @Test public void should_Accept_objects_with_a_complex_key(TestContext t) {
    ArrayList<Collection> loadCalls = new ArrayList<>();
    DataLoaderOptions options = DataLoaderOptions.create().setCacheKeyFunction(getJsonObjectCacheMapFn());
    DataLoader<JsonObject, JsonObject> identityLoader = idLoader(options, loadCalls);

    JsonObject key1 = new JsonObject().put("id", 123);
    JsonObject key2 = new JsonObject().put("id", 123);

    Future<?> future1 = identityLoader.load(key1);
    Future<?> future2 = identityLoader.load(key2);
    identityLoader.dispatch();

    join(future1, future2).onSuccess(result -> {
      assertThat(loadCalls, equalTo(Collections.singletonList(Collections.singletonList(key1))));
      assertThat(result._1, equalTo(key1));
      assertThat(result._2, equalTo(key1));
    }).respond(done(t));
  }

  @Test public void should_Clear_objects_with_complex_key(TestContext t) {
    ArrayList<Collection> loadCalls = new ArrayList<>();
    DataLoaderOptions options = DataLoaderOptions.create().setCacheKeyFunction(getJsonObjectCacheMapFn());
    DataLoader<JsonObject, JsonObject> identityLoader = idLoader(options, loadCalls);

    JsonObject key1 = new JsonObject().put("id", 123);
    JsonObject key2 = new JsonObject().put("id", 123);

    Future<JsonObject> future1 = identityLoader.load(key1);
    identityLoader.dispatch();

    future1.flatMap(f -> {
      identityLoader.clear(key2); // clear equivalent object key

      Future<JsonObject> future2 = identityLoader.load(key1);
      identityLoader.dispatch();

      return future2;
    }).onSuccess(r -> {
      assertThat(loadCalls, equalTo(asList(Collections.singletonList(key1), Collections.singletonList(key1))));
      assertThat(r, equalTo(key1));
      assertThat(r, equalTo(key1));
    }).respond(done(t));
  }

  @Test public void should_Accept_objects_with_different_order_of_keys(TestContext t) {
    ArrayList<Collection> loadCalls = new ArrayList<>();
    DataLoaderOptions options = DataLoaderOptions.create().setCacheKeyFunction(getJsonObjectCacheMapFn());
    DataLoader<JsonObject, JsonObject> identityLoader = idLoader(options, loadCalls);

    JsonObject key1 = new JsonObject().put("a", 123).put("b", 321);
    JsonObject key2 = new JsonObject().put("b", 321).put("a", 123);

    // Fetches as expected

    Future<?> future1 = identityLoader.load(key1);
    Future<?> future2 = identityLoader.load(key2);
    identityLoader.dispatch();

    join(future1, future2).onSuccess(result -> {
      assertThat(loadCalls, equalTo(Collections.singletonList(Collections.singletonList(key1))));
      assertThat(loadCalls.size(), equalTo(1));
      assertThat(result._1, equalTo(key1));
      assertThat(result._2, equalTo(key1));
    }).respond(done(t));
  }

  @Test public void should_Allow_priming_the_cache_with_an_object_key(TestContext t) {
    ArrayList<Collection> loadCalls = new ArrayList<>();
    DataLoaderOptions options = DataLoaderOptions.create().setCacheKeyFunction(getJsonObjectCacheMapFn());
    DataLoader<JsonObject, JsonObject> identityLoader = idLoader(options, loadCalls);

    JsonObject key1 = new JsonObject().put("id", 123);
    JsonObject key2 = new JsonObject().put("id", 123);

    identityLoader.prime(key1, key1);

    Future<JsonObject> future1 = identityLoader.load(key1);
    Future<JsonObject> future2 = identityLoader.load(key2);
    identityLoader.dispatch();

    join(future1, future2).onSuccess(result -> {
      assertThat(result._1, equalTo(key1));
      assertThat(result._2, equalTo(key1));
      assertThat(loadCalls, equalTo(Collections.emptyList()));
    }).respond(done(t));
  }

  @Test public void should_Accept_a_custom_cache_map_implementation(TestContext t) {
    CustomCacheMap customMap = new CustomCacheMap();
    ArrayList<Collection> loadCalls = new ArrayList<>();
    DataLoaderOptions options = DataLoaderOptions.create().setCacheMap(customMap);
    DataLoader<String, String> identityLoader = idLoader(options, loadCalls);

    // Fetches as expected

    Future<String> future1 = identityLoader.load("a");
    Future<String> future2 = identityLoader.load("b");
    identityLoader.dispatch();

    join(future1, future2).flatMap(result -> {
      assertThat(result._1, equalTo("a"));
      assertThat(result._2, equalTo("b"));

      assertThat(loadCalls, equalTo(Collections.singletonList(asList("a", "b"))));
      assertArrayEquals(customMap.stash.keySet().toArray(), asList("a", "b").toArray());

      Future<String> future3 = identityLoader.load("c");
      Future<String> future2a = identityLoader.load("b");
      identityLoader.dispatch();

      return join(future3, future2a);
    }).flatMap(result -> {
      assertThat(result._1, equalTo("c"));
      assertThat(result._2, equalTo("b"));

      assertThat(loadCalls, equalTo(asList(asList("a", "b"), Collections.singletonList("c"))));
      assertArrayEquals(customMap.stash.keySet().toArray(), asList("a", "b", "c").toArray());

      // Supports clear

      identityLoader.clear("b");
      assertArrayEquals(customMap.stash.keySet().toArray(), asList("a", "c").toArray());

      Future future2b = identityLoader.load("b");
      identityLoader.dispatch();

      return future2b;
    }).onSuccess(result -> {
      assertThat(result, equalTo("b"));
      assertThat(loadCalls, equalTo(asList(asList("a", "b"), Collections.singletonList("c"), Collections.singletonList("b"))));
      assertArrayEquals(customMap.stash.keySet().toArray(), asList("a", "c", "b").toArray());

      // Supports clear all

      identityLoader.clearAll();
      assertArrayEquals(customMap.stash.keySet().toArray(), Collections.emptyList().toArray());
    }).respond(done(t));
  }

  // It is resilient to job queue ordering

  /*
  @Test public void should_Batch_loads_occurring_within_futures(TestContext t) {
    ArrayList<Collection> loadCalls = new ArrayList<>();
    DataLoader<String, String> identityLoader = idLoader(DataLoaderOptions.create(), loadCalls);

    Future.<String>future().setHandler(rh -> {
      identityLoader.load("a");
      Future.future().setHandler(rh2 -> {
        identityLoader.load("b");
        Future.future().setHandler(rh3 -> {
          identityLoader.load("c");
          Future.future().setHandler(rh4 -> identityLoader.load("d")).complete();
        }).complete();
      }).complete();
    }).complete();
    Future<?> composite = identityLoader.dispatch();
    composite.setHandler(t.asyncAssertSuccess(r -> {
      assertThat(loadCalls, equalTo(Collections.singletonList(asList("a", "b", "c", "d"))));
    }));
  }
  */

  @Test public void should_Batch_automatically_with_explicit_context(TestContext t) {
    ArrayList<Collection> loadCalls = new ArrayList<>();
    Object context = 1L;
    Dispatcher dispatcher = new Dispatcher();
    DataLoader<Integer, Integer> identityLoader = idLoader(new DataLoaderOptions().setDispatcher(dispatcher).setContext(context), loadCalls);
    Future<Integer> future1 = identityLoader.load(1);
    Future<Integer> future2 = identityLoader.load(2);
    join(future1, future2).onSuccess(result -> {
      assertThat(result._1, equalTo(1));
      assertThat(result._2, equalTo(2));
      assertThat(loadCalls, equalTo(Collections.singletonList(asList(1, 2))));
    }).respond(done(t));
  }

  @Test public void should_Batch_automatically_between_explicit_contexts(TestContext t) {
    ArrayList<Collection> loadCalls = new ArrayList<>();
    Dispatcher dispatcher = new Dispatcher();
    // do batching for the 1st request
    DataLoader<Integer, Integer> identityLoader = idLoader(new DataLoaderOptions().setDispatcher(dispatcher).setContext(1L), loadCalls);
    Future<Integer> future1 = identityLoader.load(1);
    Future<Integer> future2 = identityLoader.load(2);
    // and the 2nd request will get separate batches
    identityLoader = idLoader(new DataLoaderOptions().setDispatcher(dispatcher).setContext(1L), loadCalls);
    Future<Integer> future3 = identityLoader.load(3);
    Future<Integer> future4 = identityLoader.load(4);
    join(future1, future2, future3, future4).onSuccess(result -> {
      assertThat(result._1, equalTo(1));
      assertThat(result._2, equalTo(2));
      assertThat(result._3, equalTo(3));
      assertThat(result._4, equalTo(4));
      assertThat(loadCalls, containsInAnyOrder(asList(1, 2), asList(3, 4)));
    }).respond(done(t));
  }

  @Test @Ignore public void should_Call_a_loader_from_a_loader() {
    // TODO Provide implementation with Futures
  }

  // Helper methods

  private static CacheKey<JsonObject> getJsonObjectCacheMapFn() {
    return key -> key.stream().map(entry -> entry.getKey() + ":" + entry.getValue()).sorted().collect(Collectors.joining());
  }

  private static class CustomCacheMap implements CacheMap<String, Object> {

    public Map<String, Object> stash;

    public CustomCacheMap() {
      stash = new LinkedHashMap<>();
    }

    @Override public boolean containsKey(String key) {
      return stash.containsKey(key);
    }

    @Override public Object get(String key) {
      return stash.get(key);
    }

    @Override public CacheMap<String, Object> set(String key, Object value) {
      stash.put(key, value);
      return this;
    }

    @Override public CacheMap<String, Object> delete(String key) {
      stash.remove(key);
      return this;
    }

    @Override public CacheMap<String, Object> clear() {
      stash.clear();
      return this;
    }
  }

  @SuppressWarnings("unchecked") private static <K> DataLoader<K, K> idLoader(DataLoaderOptions options, List<Collection> loadCalls) {
    return new DataLoader<>(keys -> {
      loadCalls.add(new ArrayList(keys));
      Promise<List<K>> result = Promise.apply();
      Vertx.currentContext().runOnContext(e -> {
        result.setValue(new ArrayList<>(keys));
      });
      return result;
    }, options);
  }

  @SuppressWarnings("unchecked") private static <K, V> DataLoader<K, V> idLoaderAllErrors(
    DataLoaderOptions options, List<Collection> loadCalls) {
    return new DataLoader<>(keys -> {
      loadCalls.add(new ArrayList(keys));
      Promise<List<V>> result = Promise.apply();
      Vertx.currentContext().runOnContext(e -> {
        result.setException(new IllegalStateException("Error"));
      });
      return result;
    }, options);
  }

  private static <T> Responder<T> done(TestContext t) {
    return new Responder<>() {
      @Override public void onException(Throwable ex) {
        t.fail(ex);
      }

      @Override public void onValue(T value) {
        t.async().complete();
      }
    };
  }

  private static <T> Responder<T> failed(TestContext t) {
    return new Responder<>() {
      @Override public void onException(Throwable ex) {
        t.async().complete();
      }

      @Override public void onValue(T value) {
        t.fail();
      }
    };
  }
}
