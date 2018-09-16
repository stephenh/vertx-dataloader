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

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
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
    DataLoader<Integer, Integer> identityLoader = new DataLoader<>(keys -> Future.succeededFuture(new ArrayList<>(keys)));

    Future<Integer> future1 = identityLoader.load(1);
    future1.setHandler(t.asyncAssertSuccess(i -> {
      assertThat(i, equalTo(1));
    }));
    identityLoader.dispatch();
  }

  @Test public void should_Support_loading_multiple_keys_in_one_call(TestContext t) {
    DataLoader<Integer, Integer> identityLoader = new DataLoader<>(keys -> Future.succeededFuture(new ArrayList<>(keys)));

    Future<List<Integer>> futureAll = identityLoader.loadMany(asList(1, 2));
    futureAll.setHandler(t.asyncAssertSuccess(r -> {
      assertThat(r.size(), is(2));
      assertThat(r, equalTo(asList(1, 2)));
    }));
    identityLoader.dispatch();
  }

  @Test public void should_Resolve_to_empty_list_when_no_keys_supplied(TestContext t) {
    Future<List<Integer>> futureEmpty = identityLoader.loadMany(Collections.emptyList());
    futureEmpty.setHandler(t.asyncAssertSuccess(r -> {
      assertThat(r.size(), is(0));
      assertThat(r, empty());
    }));
    identityLoader.dispatch();
  }

  @Test public void should_Batch_multiple_requests(TestContext t) {
    ArrayList<Collection> loadCalls = new ArrayList<>();
    DataLoader<Integer, Integer> identityLoader = idLoader(new DataLoaderOptions(), loadCalls);

    Future<Integer> future1 = identityLoader.load(1);
    Future<Integer> future2 = identityLoader.load(2);
    identityLoader.dispatch();

    CompositeFuture.join(future1, future2).setHandler(t.asyncAssertSuccess(cf -> {
      assertThat(future1.result(), equalTo(1));
      assertThat(future2.result(), equalTo(2));
      assertThat(loadCalls, equalTo(Collections.singletonList(asList(1, 2))));
    }));
  }

  @Test public void should_Coalesce_identical_requests(TestContext t) {
    ArrayList<Collection> loadCalls = new ArrayList<>();
    DataLoader<Integer, Integer> identityLoader = idLoader(new DataLoaderOptions(), loadCalls);

    Future<Integer> future1a = identityLoader.load(1);
    Future<Integer> future1b = identityLoader.load(1);
    assertThat(future1a, equalTo(future1b));
    identityLoader.dispatch();

    future1a.setHandler(t.asyncAssertSuccess(k -> {
      assertThat(future1a.result(), equalTo(1));
      assertThat(future1b.result(), equalTo(1));
      assertThat(loadCalls, equalTo(Collections.singletonList(Collections.singletonList(1))));
    }));
  }

  @Test public void should_Cache_repeated_requests(TestContext t) {
    ArrayList<Collection> loadCalls = new ArrayList<>();
    DataLoader<String, String> identityLoader = idLoader(new DataLoaderOptions(), loadCalls);

    Future<String> future1 = identityLoader.load("A");
    Future<String> future2 = identityLoader.load("B");
    identityLoader.dispatch();

    CompositeFuture.join(future1, future2).compose(then -> {
      assertThat(future1.result(), equalTo("A"));
      assertThat(future2.result(), equalTo("B"));
      assertThat(loadCalls, equalTo(Collections.singletonList(asList("A", "B"))));

      Future<String> future1a = identityLoader.load("A");
      Future<String> future3 = identityLoader.load("C");
      identityLoader.dispatch();

      return CompositeFuture.join(future1a, future3);
    }).compose(cf -> {
      assertThat(cf.resultAt(0), equalTo("A"));
      assertThat(cf.resultAt(1), equalTo("C"));
      assertThat(loadCalls, equalTo(asList(asList("A", "B"), Collections.singletonList("C"))));

      Future<String> future1b = identityLoader.load("A");
      Future<String> future2a = identityLoader.load("B");
      Future<String> future3a = identityLoader.load("C");
      identityLoader.dispatch();

      return CompositeFuture.join(future1b, future2a, future3a);
    }).setHandler(t.asyncAssertSuccess(cf -> {
      assertThat(cf.resultAt(0), equalTo("A"));
      assertThat(cf.resultAt(1), equalTo("B"));
      assertThat(cf.resultAt(2), equalTo("C"));
      assertThat(loadCalls, equalTo(asList(asList("A", "B"), Collections.singletonList("C"))));
    }));
  }

  @Test public void should_Not_redispatch_previous_load(TestContext t) {
    ArrayList<Collection> loadCalls = new ArrayList<>();
    DataLoader<String, String> identityLoader = idLoader(new DataLoaderOptions(), loadCalls);

    Future<String> future1 = identityLoader.load("A");
    identityLoader.dispatch();

    Future<String> future2 = identityLoader.load("B");
    identityLoader.dispatch();

    CompositeFuture.join(future1, future2).setHandler(t.asyncAssertSuccess(cf -> {
      assertThat(future1.result(), equalTo("A"));
      assertThat(future2.result(), equalTo("B"));
      assertThat(loadCalls, equalTo(asList(asList("A"), asList("B"))));
    }));
  }

  @Test public void should_Cache_on_redispatch(TestContext t) {
    ArrayList<Collection> loadCalls = new ArrayList<>();
    DataLoader<String, String> identityLoader = idLoader(new DataLoaderOptions(), loadCalls);

    Future<String> future1 = identityLoader.load("A");
    identityLoader.dispatch();

    Future<List<String>> future2 = identityLoader.loadMany(asList("A", "B"));
    identityLoader.dispatch();

    CompositeFuture.join(future1, future2).setHandler(t.asyncAssertSuccess(cf -> {
      assertThat(future1.result(), equalTo("A"));
      assertThat(future2.result(), equalTo(asList("A", "B")));
      assertThat(loadCalls, equalTo(asList(asList("A"), asList("B"))));
    }));
  }

  @Test public void should_Clear_single_value_in_loader(TestContext t) {
    ArrayList<Collection> loadCalls = new ArrayList<>();
    DataLoader<String, String> identityLoader = idLoader(new DataLoaderOptions(), loadCalls);

    Future<String> future1 = identityLoader.load("A");
    Future<String> future2 = identityLoader.load("B");
    identityLoader.dispatch();

    CompositeFuture.join(future1, future2).compose(cf -> {
      assertThat(future1.result(), equalTo("A"));
      assertThat(future2.result(), equalTo("B"));
      assertThat(loadCalls, equalTo(Collections.singletonList(asList("A", "B"))));

      identityLoader.clear("A");

      Future<String> future1a = identityLoader.load("A");
      Future<String> future2a = identityLoader.load("B");
      identityLoader.dispatch();

      return CompositeFuture.join(future1a, future2a);
    }).setHandler(t.asyncAssertSuccess(cf -> {
      assertThat(cf.resultAt(0), equalTo("A"));
      assertThat(cf.resultAt(1), equalTo("B"));
      assertThat(loadCalls, equalTo(asList(asList("A", "B"), Collections.singletonList("A"))));
    }));
  }

  @Test public void should_Clear_all_values_in_loader(TestContext t) {
    ArrayList<Collection> loadCalls = new ArrayList<>();
    DataLoader<String, String> identityLoader = idLoader(new DataLoaderOptions(), loadCalls);

    Future<String> future1 = identityLoader.load("A");
    Future<String> future2 = identityLoader.load("B");
    identityLoader.dispatch();

    CompositeFuture.join(future1, future2).compose(cf -> {
      assertThat(future1.result(), equalTo("A"));
      assertThat(future2.result(), equalTo("B"));
      assertThat(loadCalls, equalTo(Collections.singletonList(asList("A", "B"))));

      identityLoader.clearAll();

      Future<String> future1a = identityLoader.load("A");
      Future<String> future2a = identityLoader.load("B");
      identityLoader.dispatch();

      return CompositeFuture.join(future1a, future2a);
    }).setHandler(t.asyncAssertSuccess(cf -> {
      assertThat(cf.resultAt(0), equalTo("A"));
      assertThat(cf.resultAt(1), equalTo("B"));
      assertThat(loadCalls, equalTo(asList(asList("A", "B"), asList("A", "B"))));
    }));
  }

  @Test public void should_Allow_priming_the_cache(TestContext t) {
    ArrayList<Collection> loadCalls = new ArrayList<>();
    DataLoader<String, String> identityLoader = idLoader(new DataLoaderOptions(), loadCalls);

    identityLoader.prime("A", "A");

    Future<String> future1 = identityLoader.load("A");
    Future<String> future2 = identityLoader.load("B");
    identityLoader.dispatch();

    CompositeFuture.join(future1, future2).setHandler(t.asyncAssertSuccess(cf -> {
      assertThat(future1.result(), equalTo("A"));
      assertThat(future2.result(), equalTo("B"));
      assertThat(loadCalls, equalTo(Collections.singletonList(Collections.singletonList("B"))));
    }));
  }

  @Test public void should_Not_prime_keys_that_already_exist(TestContext t) {
    ArrayList<Collection> loadCalls = new ArrayList<>();
    DataLoader<String, String> identityLoader = idLoader(new DataLoaderOptions(), loadCalls);

    identityLoader.prime("A", "X");

    Future<String> future1 = identityLoader.load("A");
    Future<String> future2 = identityLoader.load("B");
    Future<?> composite = identityLoader.dispatch();

    composite.compose(c -> {
      assertThat(future1.result(), equalTo("X"));
      assertThat(future2.result(), equalTo("B"));

      identityLoader.prime("A", "Y");
      identityLoader.prime("B", "Y");

      Future<String> future1a = identityLoader.load("A");
      Future<String> future2a = identityLoader.load("B");
      identityLoader.dispatch();

      return CompositeFuture.join(future1a, future2a);
    }).setHandler(t.asyncAssertSuccess(cf -> {
      assertThat(cf.resultAt(0), equalTo("X"));
      assertThat(cf.resultAt(1), equalTo("B"));
      assertThat(loadCalls, equalTo(Collections.singletonList(Collections.singletonList("B"))));
    }));
  }

  @Test public void should_Allow_to_forcefully_prime_the_cache(TestContext t) {
    ArrayList<Collection> loadCalls = new ArrayList<>();
    DataLoader<String, String> identityLoader = idLoader(new DataLoaderOptions(), loadCalls);

    identityLoader.prime("A", "X");

    Future<String> future1 = identityLoader.load("A");
    Future<String> future2 = identityLoader.load("B");
    Future<?> composite = identityLoader.dispatch();

    composite.compose(f -> {
      assertThat(future1.result(), equalTo("X"));
      assertThat(future2.result(), equalTo("B"));

      identityLoader.clear("A").prime("A", "Y");
      identityLoader.clear("B").prime("B", "Y");

      Future<String> future1a = identityLoader.load("A");
      Future<String> future2a = identityLoader.load("B");
      identityLoader.dispatch();

      return CompositeFuture.join(future1a, future2a);
    }).setHandler(t.asyncAssertSuccess(cf -> {
      assertThat(cf.resultAt(0), equalTo("Y"));
      assertThat(cf.resultAt(1), equalTo("Y"));
      assertThat(loadCalls, equalTo(Collections.singletonList(Collections.singletonList("B"))));
    }));
  }

  @Test public void should_Cache_failed_fetches(TestContext t) {
    ArrayList<Collection> loadCalls = new ArrayList<>();
    DataLoader<Integer, Integer> errorLoader = idLoaderAllErrors(new DataLoaderOptions(), loadCalls);

    Future<Integer> future1 = errorLoader.load(1);
    errorLoader.dispatch();

    future1.compose(f -> {
      assertThat(future1.failed(), is(true));
      assertThat(future1.cause(), instanceOf(IllegalStateException.class));

      Future<Integer> future2 = errorLoader.load(1);
      errorLoader.dispatch();

      return future2;
    }).setHandler(t.asyncAssertFailure(cause -> {
      assertThat(cause, instanceOf(IllegalStateException.class));
      assertThat(loadCalls, equalTo(Collections.singletonList(Collections.singletonList(1))));
    }));
  }

  @Test public void should_Handle_priming_the_cache_with_an_error(TestContext t) {
    ArrayList<Collection> loadCalls = new ArrayList<>();
    DataLoader<Integer, Integer> identityLoader = idLoader(new DataLoaderOptions(), loadCalls);

    identityLoader.prime(1, new IllegalStateException("Error"));

    Future<Integer> future1 = identityLoader.load(1);
    identityLoader.dispatch();

    future1.setHandler(t.asyncAssertFailure(cause -> {
      assertThat(cause, instanceOf(IllegalStateException.class));
      assertThat(loadCalls, equalTo(Collections.emptyList()));
    }));
  }

  @Test public void should_Clear_values_from_cache_after_errors(TestContext t) {
    ArrayList<Collection> loadCalls = new ArrayList<>();
    DataLoader<Integer, Integer> errorLoader = idLoaderAllErrors(new DataLoaderOptions(), loadCalls);

    Future<Integer> future1 = errorLoader.load(1);
    future1.setHandler(rh -> {
      if (rh.failed()) {
        // Presumably determine if this error is transient, and only clear the cache in that case.
        errorLoader.clear(1);
      }

      assertThat(future1.failed(), is(true));
      assertThat(future1.cause(), instanceOf(IllegalStateException.class));

      Future<Integer> future2 = errorLoader.load(1);
      future2.setHandler(rh2 -> {
        if (rh2.failed()) {
          // Again, only do this if you can determine the error is transient.
          errorLoader.clear(1);
        }
        assertThat(rh2.cause(), instanceOf(IllegalStateException.class));
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
    Future<Integer> future2 = errorLoader.load(2);
    errorLoader.dispatch();

    CompositeFuture.join(future1, future2).setHandler(ar -> {
      assertThat(future1.failed(), is(true));
      Throwable cause = future1.cause();
      assertThat(cause, instanceOf(IllegalStateException.class));
      assertThat(cause.getMessage(), equalTo("Error"));

      cause = future2.cause();
      assertThat(cause.getMessage(), equalTo(cause.getMessage()));
      assertThat(loadCalls, equalTo(Collections.singletonList(asList(1, 2))));
    });
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

    identityLoader.dispatch().setHandler(rh -> {
      assertThat(rh.succeeded(), is(true));
      assertThat(rh.result().get(0), equalTo(keyA));
      assertThat(rh.result().get(1), equalTo(keyB));
    });

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

    identityLoader.dispatch().setHandler(rh -> {
      assertThat(rh.succeeded(), is(true));
      assertThat(rh.result().get(0), equalTo(keyA));
      assertThat(identityLoader.getCacheKey(keyB), equalTo(keyB));
    });

    assertThat(loadCalls.size(), equalTo(2));
    assertThat(loadCalls.get(1).size(), equalTo(1));
    assertThat(loadCalls.get(1).toArray()[0], equalTo(keyA));
  }

  // Accepts options

  @Test public void should_Disable_caching(TestContext t) {
    ArrayList<Collection> loadCalls = new ArrayList<>();
    DataLoader<String, String> identityLoader = idLoader(DataLoaderOptions.create().setCachingEnabled(false), loadCalls);

    Future<String> future1 = identityLoader.load("A");
    Future<String> future2 = identityLoader.load("B");
    identityLoader.dispatch();

    CompositeFuture.join(future1, future2).compose(cf -> {
      assertThat(future1.result(), equalTo("A"));
      assertThat(future2.result(), equalTo("B"));
      assertThat(loadCalls, equalTo(Collections.singletonList(asList("A", "B"))));

      Future<String> future1a = identityLoader.load("A");
      Future<String> future3 = identityLoader.load("C");
      identityLoader.dispatch();

      return CompositeFuture.join(future1a, future3);
    }).compose(cf -> {
      assertThat(cf.resultAt(0), equalTo("A"));
      assertThat(cf.resultAt(1), equalTo("C"));
      assertThat(loadCalls, equalTo(asList(asList("A", "B"), asList("A", "C"))));

      Future<String> future1b = identityLoader.load("A");
      Future<String> future2a = identityLoader.load("B");
      Future<String> future3a = identityLoader.load("C");
      identityLoader.dispatch();

      return CompositeFuture.join(future1b, future2a, future3a);
    }).setHandler(t.asyncAssertSuccess(cf -> {
      assertThat(cf.resultAt(0), equalTo("A"));
      assertThat(cf.resultAt(1), equalTo("B"));
      assertThat(cf.resultAt(2), equalTo("C"));
      assertThat(loadCalls, equalTo(asList(asList("A", "B"), asList("A", "C"), asList("A", "B", "C"))));
    }));
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

    CompositeFuture.join(future1, future2).setHandler(t.asyncAssertSuccess(cf -> {
      assertThat(loadCalls, equalTo(Collections.singletonList(Collections.singletonList(key1))));
      assertThat(future1.result(), equalTo(key1));
      assertThat(future2.result(), equalTo(key1));
    }));
  }

  @Test public void should_Clear_objects_with_complex_key(TestContext t) {
    ArrayList<Collection> loadCalls = new ArrayList<>();
    DataLoaderOptions options = DataLoaderOptions.create().setCacheKeyFunction(getJsonObjectCacheMapFn());
    DataLoader<JsonObject, JsonObject> identityLoader = idLoader(options, loadCalls);

    JsonObject key1 = new JsonObject().put("id", 123);
    JsonObject key2 = new JsonObject().put("id", 123);

    Future<JsonObject> future1 = identityLoader.load(key1);
    identityLoader.dispatch();

    future1.compose(f -> {
      identityLoader.clear(key2); // clear equivalent object key

      Future<JsonObject> future2 = identityLoader.load(key1);
      identityLoader.dispatch();

      return future2;
    }).setHandler(t.asyncAssertSuccess(r -> {
      assertThat(loadCalls, equalTo(asList(Collections.singletonList(key1), Collections.singletonList(key1))));
      assertThat(future1.result(), equalTo(key1));
      assertThat(r, equalTo(key1));
    }));
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

    CompositeFuture.join(future1, future2).setHandler(t.asyncAssertSuccess(cf -> {
      assertThat(loadCalls, equalTo(Collections.singletonList(Collections.singletonList(key1))));
      assertThat(loadCalls.size(), equalTo(1));
      assertThat(future1.result(), equalTo(key1));
      assertThat(future2.result(), equalTo(key1));
    }));
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

    CompositeFuture.join(future1, future2).setHandler(t.asyncAssertSuccess(cf -> {
      assertThat(loadCalls, equalTo(Collections.emptyList()));
      assertThat(future1.result(), equalTo(key1));
      assertThat(future2.result(), equalTo(key1));
    }));
  }

  @Test public void should_Accept_a_custom_cache_map_implementation(TestContext t) {
    CustomCacheMap customMap = new CustomCacheMap();
    ArrayList<Collection> loadCalls = new ArrayList<>();
    DataLoaderOptions options = DataLoaderOptions.create().setCacheMap(customMap);
    DataLoader<String, String> identityLoader = idLoader(options, loadCalls);

    // Fetches as expected

    Future future1 = identityLoader.load("a");
    Future future2 = identityLoader.load("b");
    Future<?> composite = identityLoader.dispatch();

    composite.compose(f -> {
      assertThat(future1.result(), equalTo("a"));
      assertThat(future2.result(), equalTo("b"));

      assertThat(loadCalls, equalTo(Collections.singletonList(asList("a", "b"))));
      assertArrayEquals(customMap.stash.keySet().toArray(), asList("a", "b").toArray());

      Future future3 = identityLoader.load("c");
      Future future2a = identityLoader.load("b");
      identityLoader.dispatch();

      return CompositeFuture.join(future3, future2a);
    }).compose(cf -> {
      assertThat(cf.resultAt(0), equalTo("c"));
      assertThat(cf.resultAt(1), equalTo("b"));

      assertThat(loadCalls, equalTo(asList(asList("a", "b"), Collections.singletonList("c"))));
      assertArrayEquals(customMap.stash.keySet().toArray(), asList("a", "b", "c").toArray());

      // Supports clear

      identityLoader.clear("b");
      assertArrayEquals(customMap.stash.keySet().toArray(), asList("a", "c").toArray());

      Future future2b = identityLoader.load("b");
      identityLoader.dispatch();

      return future2b;
    }).setHandler(t.asyncAssertSuccess(r -> {
      assertThat(r, equalTo("b"));
      assertThat(loadCalls, equalTo(asList(asList("a", "b"), Collections.singletonList("c"), Collections.singletonList("b"))));
      assertArrayEquals(customMap.stash.keySet().toArray(), asList("a", "c", "b").toArray());

      // Supports clear all

      identityLoader.clearAll();
      assertArrayEquals(customMap.stash.keySet().toArray(), Collections.emptyList().toArray());
    }));
  }

  // It is resilient to job queue ordering

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

  @Test public void should_Batch_automatically_with_explicit_context(TestContext t) {
    ArrayList<Collection> loadCalls = new ArrayList<>();
    Object context = 1L;
    Dispatcher dispatcher = new Dispatcher();
    DataLoader<Integer, Integer> identityLoader = idLoader(new DataLoaderOptions().setDispatcher(dispatcher).setContext(context), loadCalls);
    Future<Integer> future1 = identityLoader.load(1);
    Future<Integer> future2 = identityLoader.load(2);
    CompositeFuture.join(future1, future2).setHandler(t.asyncAssertSuccess(ar -> {
      assertThat(future1.result(), equalTo(1));
      assertThat(future2.result(), equalTo(2));
      assertThat(loadCalls, equalTo(Collections.singletonList(asList(1, 2))));
    }));
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
    CompositeFuture.join(future1, future2, future3, future4).setHandler(t.asyncAssertSuccess(ar -> {
      assertThat(future1.result(), equalTo(1));
      assertThat(future2.result(), equalTo(2));
      assertThat(future3.result(), equalTo(3));
      assertThat(future4.result(), equalTo(4));
      assertThat(loadCalls, containsInAnyOrder(asList(1, 2), asList(3, 4)));
    }));
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
      Future<List<K>> result = Future.future();
      Vertx.currentContext().runOnContext(e -> {
        result.complete(new ArrayList<>(keys));
      });
      return result;
    }, options);
  }

  @SuppressWarnings("unchecked") private static <K, V> DataLoader<K, V> idLoaderAllErrors(
    DataLoaderOptions options, List<Collection> loadCalls) {
    return new DataLoader<>(keys -> {
      loadCalls.add(new ArrayList(keys));
      Future<List<V>> result = Future.future();
      Vertx.currentContext().runOnContext(e -> {
        result.fail(new IllegalStateException("Error"));
      });
      return result;
    }, options);
  }
}
