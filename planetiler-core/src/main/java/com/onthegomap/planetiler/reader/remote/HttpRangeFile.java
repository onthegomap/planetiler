package com.onthegomap.planetiler.reader.remote;

import com.onthegomap.planetiler.util.CompletableFutures;
import com.onthegomap.planetiler.util.Downloader;
import com.onthegomap.planetiler.util.Downloader.ByteRange;
import com.onthegomap.planetiler.util.Downloader.RangeResponse;
import com.onthegomap.planetiler.util.Downloader.RemoteFileChangedException;
import com.onthegomap.planetiler.util.Downloader.RemoteVersion;
import com.onthegomap.planetiler.util.RequestPlanner;
import com.onthegomap.planetiler.util.RequestPlanner.Plan;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

/**
 * A {@link RangeFile} that reads a remote file with HTTP range requests through {@link Downloader}, without storing
 * anything on disk.
 * <p>
 * Each response gets checked against the version of the file seen when it was opened, and reads fail with a
 * {@link RemoteFileChangedException} if the file changed since then (only its length gets checked with
 * {@code download_fail_if_changed=false}).
 */
public class HttpRangeFile implements RangeFile {

  private final Downloader downloader;
  private final String url;
  private final RemoteVersion version;
  private final RangeResponse prefetched;
  private final LongAdder bytesFetched = new LongAdder();
  private final LongAdder requestsMade = new LongAdder();
  private final Set<ResultDispatcher> inProgress = ConcurrentHashMap.newKeySet();

  private HttpRangeFile(Downloader downloader, RangeResponse prefetched) {
    this.downloader = downloader;
    this.url = prefetched.url();
    this.version = prefetched.version();
    this.prefetched = prefetched;
    bytesFetched.add(prefetched.bytes().length);
    requestsMade.increment();
  }

  /**
   * Opens {@code url} for reading, fetching its length and validators with a request for its first byte.
   *
   * @throws IOException                            if the request fails
   * @throws Downloader.RangesNotSupportedException if the server does not support range requests
   */
  public static HttpRangeFile open(Downloader downloader, String url) throws IOException {
    return open(downloader, url, 0);
  }

  /**
   * Opens {@code url} like {@link #open(Downloader, String)}, but requests its last {@code tailBytes} bytes instead of
   * the first one if {@code tailBytes > 0}, for formats like GeoParquet that keep their metadata at the end of the
   * file.
   *
   * @throws IOException                            if the request fails
   * @throws Downloader.RangesNotSupportedException if the server does not support range requests
   */
  public static HttpRangeFile open(Downloader downloader, String url, int tailBytes) throws IOException {
    return new HttpRangeFile(downloader,
      tailBytes > 0 ? downloader.fetchTail(url, tailBytes) : downloader.fetchRange(url, new ByteRange(0, 1)));
  }

  /** Returns the URL of the file after following redirects. */
  public String url() {
    return url;
  }

  /** Returns the version of the remote file seen when it was opened. */
  public RemoteVersion version() {
    return version;
  }

  /**
   * Returns true if {@code other} is the same version of the file as this one: same length and, unless
   * {@code download_fail_if_changed=false}, the same validators.
   */
  boolean isSameVersion(RemoteVersion other) {
    return downloader.isSameVersion(version, other);
  }

  /** Returns the bytes fetched by the request that opened the file. */
  RangeResponse prefetched() {
    return prefetched;
  }

  private boolean isPrefetched(long offset, long length) {
    return offset >= prefetched.offset() && offset + length <= prefetched.offset() + prefetched.bytes().length;
  }

  @Override
  public long length() {
    return version.length();
  }

  @Override
  public byte[] read(long offset, int length) throws IOException {
    return readLater(List.of(new ByteRange(offset, length))).getFirst().get();
  }

  @Override
  public boolean needsFetch(long offset, long length) {
    return !isPrefetched(offset, length);
  }

  @Override
  public Plan planFetch(List<ByteRange> ranges) {
    return downloader.planner()
      .plan(ranges.stream().filter(range -> needsFetch(range.offset(), range.length())).toList(), length());
  }

  @Override
  public List<Pending> readLater(List<ByteRange> ranges) {
    return readLater(ranges, null);
  }

  /**
   * Like {@link #readLater(List)}, but also passes every byte fetched to {@code byteListener}, including gaps between
   * ranges (e.g. for {@link CachedRangeFile} to store them on disk).
   */
  List<Pending> readLater(List<ByteRange> ranges, Downloader.Sink byteListener) {
    List<Pending> result = new ArrayList<>();
    List<RequestedRange> rangesToFetch = new ArrayList<>();
    for (var range : ranges) {
      if (range.offset() < 0 || range.length() < 0 || range.end() > version.length()) {
        throw new IllegalArgumentException("Range " + range + " out of bounds for " + url + " (" + version.length() +
          " bytes)");
      }
      if (isPrefetched(range.offset(), range.length())) {
        int start = Math.toIntExact(range.offset() - prefetched.offset());
        byte[] bytes = Arrays.copyOfRange(prefetched.bytes(), start, start + Math.toIntExact(range.length()));
        result.add(() -> bytes);
      } else {
        var toFetch = new RequestedRange(range);
        rangesToFetch.add(toFetch);
        result.add(toFetch);
      }
    }
    var dispatcher = new ResultDispatcher(rangesToFetch, byteListener);
    inProgress.add(dispatcher);
    Plan plan = downloader.planner().plan(rangesToFetch.stream().map(RequestedRange::range).toList(), length());
    fetch(plan, dispatcher).whenComplete((_, error) -> {
      inProgress.remove(dispatcher);
      if (error != null) {
        dispatcher.fail(error);
      }
    });
    return result;
  }

  /**
   * Starts fetching {@code ranges} of the file in the coalesced ranges {@link RequestPlanner} plans for them, passing
   * every byte fetched to {@code byteListener} as it arrives, including gaps between ranges.
   */
  CompletableFuture<Void> fetch(List<ByteRange> ranges, Downloader.Sink byteListener) {
    return fetch(downloader.planner().plan(ranges, length()), new ResultDispatcher(List.of(), byteListener));
  }

  /**
   * Requests the coalesced ranges of {@code plan} in file order, at most {@link Plan#maxInFlight()} at once, each once
   * the caller starts the first requested range of {@code dispatcher} in it.
   */
  private CompletableFuture<Void> fetch(Plan plan, ResultDispatcher dispatcher) {
    if (plan.httpRequests().isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }
    return downloader.async(() -> {
      var inFlight = new Semaphore(plan.maxInFlight());
      List<CompletableFuture<?>> requests = new ArrayList<>();
      // planner coalesces small requested ranges into fewer, larger ranges (potentially with gaps in them)
      for (var coalescedRange : plan.httpRequests()) {
        // wait for caller to call start() on a range inside this coalesced range before taking a slot or allocating memory
        var rangeSplitter = dispatcher.awaitSplitter(coalescedRange);
        inFlight.acquire();
        // when a request fails for good, stop trying the rest of this file
        if (dispatcher.failed) {
          inFlight.release();
          break;
        }
        requestsMade.increment();
        requests.add(downloader.async(() -> {
          try {
            downloader.fetch(url, version, coalescedRange, (position, buffer, offset, length) -> {
              bytesFetched.add(length);
              rangeSplitter.accept(position, buffer, offset, length);
            });
            return null;
          } catch (IOException | RuntimeException e) {
            dispatcher.fail(e);
            throw e;
          } finally {
            inFlight.release();
          }
        }));
      }
      CompletableFutures.awaitIO(CompletableFutures.joinFutures(requests));
      return null;
    });
  }

  @Override
  public long bytesFetched() {
    return bytesFetched.sum();
  }

  @Override
  public long requestsMade() {
    return requestsMade.sum();
  }

  /** Fails the requested ranges still waiting for their bytes; requests in flight stop once they get to one. */
  @Override
  public void close() {
    for (var dispatcher : inProgress) {
      dispatcher.fail(new CancellationException(url + " closed"));
    }
  }

  /**
   * Hands the bytes of the coalesced ranges fetched for one {@link #readLater} call to the requested ranges they cover,
   * and every byte, gaps included, to {@code byteListener} if not null.
   */
  private static final class ResultDispatcher {

    private final List<RequestedRange> requestedRanges;
    private final Downloader.Sink byteListener;
    private volatile boolean failed = false;

    ResultDispatcher(List<RequestedRange> requestedRanges, Downloader.Sink byteListener) {
      this.requestedRanges = requestedRanges;
      this.byteListener = byteListener;
    }

    /**
     * Waits until the caller starts the first requested range in {@code coalescedRange}, then returns a sink that
     * splits its bytes between {@code byteListener} and the requested ranges it covers.
     */
    Downloader.Sink awaitSplitter(ByteRange coalescedRange) throws IOException {
      Queue<RequestedRange> covered = requestedRanges.stream()
        .filter(requested -> overlap(requested.range, coalescedRange.offset(), coalescedRange.end()) > 0)
        .collect(Collectors.toCollection(ArrayDeque::new));
      if (!covered.isEmpty()) {
        CompletableFutures.awaitIO(covered.peek().started);
      }
      return (filePosition, chunk, chunkOffset, chunkLength) -> {
        if (byteListener != null) {
          byteListener.accept(filePosition, chunk, chunkOffset, chunkLength);
        }
        long chunkEnd = filePosition + chunkLength;
        while (!covered.isEmpty() && covered.peek().range.offset() < chunkEnd) {
          var requested = covered.peek();
          long copyFrom = Math.max(filePosition, requested.range.offset());
          int copyOffsetInChunk = chunkOffset + (int) (copyFrom - filePosition);
          int copyLength = (int) overlap(requested.range, filePosition, chunkEnd);
          requested.accept(copyFrom, chunk, copyOffsetInChunk, copyLength);
          if (requested.range.end() > chunkEnd) {
            // the rest of this range comes in the next chunk
            break;
          }
          // bytes arrive in order, so a range that ended in this chunk gets nothing from the chunks after it
          covered.remove();
        }
      };
    }

    private static long overlap(ByteRange range, long fromFilePosition, long toFilePosition) {
      return Math.max(0, Math.min(range.end(), toFilePosition) - Math.max(range.offset(), fromFilePosition));
    }

    void fail(Throwable error) {
      failed = true;
      for (var requested : requestedRanges) {
        requested.fail(error);
      }
    }
  }

  /**
   * One range requested by a caller, which may be a small part of a larger coalesced range request made to the remote
   * server.
   */
  private static final class RequestedRange implements Pending {

    private final ByteRange range;
    private final CompletableFuture<Void> started = new CompletableFuture<>();
    private final CompletableFuture<Void> done = new CompletableFuture<>();
    private byte[] bytes;
    private long remaining;

    RequestedRange(ByteRange range) {
      this.range = range;
      this.remaining = range.length();
      if (remaining == 0) {
        bytes = new byte[0];
        done.complete(null);
      }
    }

    ByteRange range() {
      return range;
    }

    @Override
    public void start() {
      started.complete(null);
    }

    @Override
    public CompletableFuture<Void> ready() {
      return done.copy(); // don't let callers complete it
    }

    @Override
    public byte[] get() throws IOException {
      start();
      CompletableFutures.awaitIO(done);
      synchronized (this) {
        byte[] result = bytes;
        bytes = null;
        return result;
      }
    }

    void accept(long filePosition, byte[] chunk, int chunkOffset, int length) {
      // block the HTTP request fetching these bytes until the caller calls start()
      started.join();
      synchronized (this) {
        if (bytes == null) {
          bytes = new byte[Math.toIntExact(range.length())];
        }
        int offsetInRange = Math.toIntExact(filePosition - range.offset());
        System.arraycopy(chunk, chunkOffset, bytes, offsetInRange, length);
        remaining -= length;
        if (remaining == 0) {
          done.complete(null);
        }
      }
    }

    void fail(Throwable error) {
      started.completeExceptionally(error);
      done.completeExceptionally(error);
    }
  }

  @Override
  public String toString() {
    return url;
  }
}
