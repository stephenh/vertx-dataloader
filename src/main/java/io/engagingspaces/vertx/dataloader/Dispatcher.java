package io.engagingspaces.vertx.dataloader;

import io.vertx.core.Vertx;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Automatically dispatches pending requests when the "context" changes.
 *
 * Context is a caller-provided handle that identities the current request/unit of work.
 *
 * These per-request contexts can be passed into {@link DataLoaderOptions} and then a {@link Dispatcher}
 * instance will watch for "has the context changed?" and flush any pending callers.
 *
 * Note that just watching for "new context" is not sufficient, as in low-volume services there
 * may not be a next request come in immediately, so we make a Vert.x-specific implementation
 * assumption and schedule a flush command on the current Vert.x context/thread. This flush
 * will be put at the back of the queue, so the currently-queued futures will have a chance
 * to batch their operations.
 */
public class Dispatcher {

  private final Set<DataLoader<?, ?>> pending = new HashSet<>();
  private boolean flushQueued = false;
  private Object currentContext;

  public void queue(DataLoader<?, ?> dl, Object context) {
    if (context != currentContext) {
      dispatchPending();
      currentContext = context;
    }
    this.pending.add(dl);
    if (!flushQueued) {
      Vertx.currentContext().runOnContext((e) -> flush());
      flushQueued = true;
    }
  }

  private void flush() {
    flushQueued = false;
    dispatchPending();
  }

  private void dispatchPending() {
    List<DataLoader<?, ?>> copy = new ArrayList<>(pending);
    pending.clear();
    copy.forEach(DataLoader::dispatch);
  }
}
