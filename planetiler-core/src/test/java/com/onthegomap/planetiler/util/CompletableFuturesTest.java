package com.onthegomap.planetiler.util;

import static org.junit.jupiter.api.Assertions.*;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

class CompletableFuturesTest {

  private static class Resource implements Closeable {
    boolean closed = false;
    final IOException closeError;

    Resource(IOException closeError) {
      this.closeError = closeError;
    }

    Resource() {
      this(null);
    }

    @Override
    public void close() throws IOException {
      closed = true;
      if (closeError != null) {
        throw closeError;
      }
    }
  }

  @Test
  void testAwaitReturnsResult() throws IOException {
    assertEquals("a", CompletableFutures.await(CompletableFuture.completedFuture("a")));
    assertEquals("a", CompletableFutures.awaitIO(CompletableFuture.completedFuture("a")));
  }

  @Test
  void testAwaitThrowsWithoutWrapper() {
    var runtime = new IllegalStateException("runtime");
    var failedRuntime = CompletableFuture.failedFuture(runtime);
    assertSame(runtime, assertThrows(IllegalStateException.class, () -> CompletableFutures.await(failedRuntime)));
    var io = new IOException("io");
    var failedIO = CompletableFuture.failedFuture(io);
    assertSame(io, assertThrows(UncheckedIOException.class, () -> CompletableFutures.await(failedIO)).getCause());
  }

  @Test
  void testAwaitIOThrowsIOExceptions() {
    var io = new IOException("io");
    var failedIO = CompletableFuture.failedFuture(io);
    assertSame(io, assertThrows(IOException.class, () -> CompletableFutures.awaitIO(failedIO)));
    var failedUnchecked = CompletableFuture.failedFuture(new UncheckedIOException(io));
    assertSame(io, assertThrows(IOException.class, () -> CompletableFutures.awaitIO(failedUnchecked)));
    var runtime = new IllegalStateException("runtime");
    var failedRuntime = CompletableFuture.failedFuture(runtime);
    assertSame(runtime, assertThrows(IllegalStateException.class, () -> CompletableFutures.awaitIO(failedRuntime)));
  }

  @Test
  void testJoinAllOrCloseReturnsResultsInOrder() throws IOException {
    var a = new Resource();
    var b = new Resource();
    var later = new CompletableFuture<Resource>();
    CompletableFuture.runAsync(() -> later.complete(a));
    assertEquals(List.of(a, b),
      CompletableFutures.joinAllOrClose(List.of(later, CompletableFuture.completedFuture(b))));
    assertFalse(a.closed);
    assertFalse(b.closed);
  }

  @Test
  void testJoinAllOrCloseClosesOpenedOnFailure() {
    var a = new Resource();
    var c = new Resource(new IOException("close c"));
    var first = new IllegalStateException("first");
    List<CompletableFuture<Resource>> futures = List.of(
      CompletableFuture.completedFuture(a),
      CompletableFuture.failedFuture(first),
      CompletableFuture.completedFuture(c),
      CompletableFuture.failedFuture(new IOException("second")));

    var thrown = assertThrows(IllegalStateException.class, () -> CompletableFutures.joinAllOrClose(futures));
    assertSame(first, thrown, "the first failure in list order");
    assertTrue(a.closed);
    assertTrue(c.closed);
    assertEquals("close c", thrown.getSuppressed()[0].getMessage());
  }

  @Test
  void testJoinFuturesFailsFast() {
    var never = new CompletableFuture<Void>();
    var joined = CompletableFutures.joinFutures(never, CompletableFuture.failedFuture(new IOException("fail")));
    assertTrue(joined.isCompletedExceptionally());
    assertTrue(never.isCancelled());
  }
}
