package com.onthegomap.planetiler.reader.remote;

import com.onthegomap.planetiler.util.CompletableFutures;
import com.onthegomap.planetiler.util.Downloader;
import com.onthegomap.planetiler.util.Downloader.ByteRange;
import com.onthegomap.planetiler.util.Downloader.RangeResponse;
import com.onthegomap.planetiler.util.Downloader.RemoteFileChangedException;
import com.onthegomap.planetiler.util.Downloader.RemoteVersion;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.LongAdder;

/**
 * A {@link RangeFile} that reads a remote file with HTTP range requests through {@link Downloader}, without storing
 * anything on disk.
 * <p>
 * Large reads get split into several requests made in parallel ({@link Downloader#fetchRanges}). Each response gets
 * checked against the version of the file seen when it was opened, and reads fail with a
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

  private boolean isPrefetched(long offset, int length) {
    return offset >= prefetched.offset() && offset + length <= prefetched.offset() + prefetched.bytes().length;
  }

  @Override
  public long length() {
    return version.length();
  }

  @Override
  public byte[] read(long offset, int length) throws IOException {
    return readLater(offset, length).get();
  }

  @Override
  public Pending readLater(long offset, int length) {
    if (offset < 0 || length < 0 || offset + length > version.length()) {
      throw new IllegalArgumentException(
        "Range " + offset + "+" + length + " out of bounds for " + url + " (" + version.length() + " bytes)");
    }
    if (isPrefetched(offset, length)) {
      int start = Math.toIntExact(offset - prefetched.offset());
      byte[] result = Arrays.copyOfRange(prefetched.bytes(), start, start + length);
      return () -> result;
    }
    var ranges = List.of(new ByteRange(offset, length));
    var bytes = downloader.fetchBytes(url, version, ranges.getFirst(), bytesFetched::add)
      .whenComplete((_, _) -> requestsMade.add(countRequests(ranges)));
    return () -> CompletableFutures.awaitIO(bytes);
  }

  /**
   * Starts fetching {@code ranges} of the file, passing the bytes to {@code sink} as they arrive (see
   * {@link Downloader#fetchRanges}).
   */
  CompletableFuture<Void> fetch(List<ByteRange> ranges, Downloader.Sink sink) {
    return downloader.fetchRanges(url, version, ranges, (position, buffer, offset, length) -> {
      sink.accept(position, buffer, offset, length);
      bytesFetched.add(length);
    }).thenRun(() -> requestsMade.add(countRequests(ranges)));
  }

  // requests made, not counting retries
  private int countRequests(List<ByteRange> ranges) {
    return downloader.planRequests(ranges).size();
  }

  // these mirror readLater: a read within the prefetched bytes fetches nothing, any other read fetches all of it
  @Override
  public long bytesToFetch(long offset, int length) {
    return isPrefetched(offset, length) ? 0 : length;
  }

  @Override
  public long requestsToFetch(long offset, int length) {
    return isPrefetched(offset, length) ? 0 : countRequests(List.of(new ByteRange(offset, length)));
  }

  @Override
  public long bytesFetched() {
    return bytesFetched.sum();
  }

  @Override
  public long requestsMade() {
    return requestsMade.sum();
  }

  @Override
  public void close() {
    // nothing to release: requests in flight finish on their own
  }
}
