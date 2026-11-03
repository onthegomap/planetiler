package com.onthegomap.planetiler.reader.remote;

import com.onthegomap.planetiler.util.CompletableFutures;
import com.onthegomap.planetiler.util.Downloader;
import com.onthegomap.planetiler.util.Downloader.RangeResponse;
import com.onthegomap.planetiler.util.Downloader.RemoteFileChangedException;
import com.onthegomap.planetiler.util.Downloader.RemoteVersion;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.LongAdder;

/**
 * A {@link RangeFile} that reads a remote file with HTTP range requests through {@link Downloader}, without storing
 * anything on disk.
 * <p>
 * Reads larger than {@code download_range_chunk_size_mb} get split into several requests made in parallel. Each
 * response gets checked against the version of the file seen when it was opened, and reads fail with a
 * {@link RemoteFileChangedException} if the file changed since then (only its length gets checked with
 * {@code download_fail_if_changed=false}).
 */
public class HttpRangeFile implements RangeFile {

  /**
   * Cloud-optimized formats like GeoParquet keep their metadata at the end of the file, so fetching a generous chunk of
   * the end up front with the request that gets the file's length avoids one round trip per metadata read.
   */
  static final int TAIL_BYTES = 1 << 20;
  private final Downloader downloader;
  private final String url;
  private final RemoteVersion version;
  private final byte[] tail;
  private final long tailOffset;
  private final int chunkBytes;
  private final boolean failIfChanged;
  private final LongAdder bytesFetched = new LongAdder();
  private final LongAdder requestsMade = new LongAdder();

  private HttpRangeFile(Downloader downloader, RangeResponse tail) {
    var config = downloader.config();
    this.downloader = downloader;
    this.url = tail.url();
    this.version = tail.version();
    this.tail = tail.bytes();
    this.tailOffset = tail.offset();
    this.chunkBytes = Math.toIntExact(Math.min(Integer.MAX_VALUE, config.downloadRangeChunkSizeMB() * 1_000_000L));
    this.failIfChanged = config.downloadFailIfChanged();
    countResponse(tail);
  }

  /**
   * Opens {@code url} for reading, fetching its length, validators and last bytes.
   *
   * @throws IOException if the request fails
   */
  public static HttpRangeFile open(Downloader downloader, String url) throws IOException {
    return open(downloader, url, TAIL_BYTES);
  }

  /** Opens {@code url} like {@link #open(Downloader, String)} but only fetches its last {@code tailBytes} bytes. */
  static HttpRangeFile open(Downloader downloader, String url, int tailBytes) throws IOException {
    return new HttpRangeFile(downloader, downloader.fetchTail(url, tailBytes));
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
    return failIfChanged ? version.matches(other) : version.length() == other.length();
  }

  /** Returns the last bytes of the file, fetched when it was opened. */
  byte[] tail() {
    return tail;
  }

  long tailOffset() {
    return tailOffset;
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
    if (offset >= tailOffset) {
      int start = Math.toIntExact(offset - tailOffset);
      byte[] result = Arrays.copyOfRange(tail, start, start + length);
      return () -> result;
    }
    return fetchInChunks(offset, length);
  }

  private Pending fetchInChunks(long offset, int length) {
    byte[] result = new byte[length];
    // one connection is limited by its round trip time (TCP ramp-up), so a large range goes over several at once
    List<CompletableFuture<Void>> chunks = new ArrayList<>();
    for (int start = 0; start < length; start += chunkBytes) {
      int chunkStart = start;
      int chunkLength = Math.min(chunkBytes, length - start);
      chunks.add(downloader.fetchRange(url, failIfChanged ? version : null, offset + start, chunkLength)
        .thenAccept(response -> {
          // double check precondition in case server does not honor or if file has no last-modified or etag
          if (!isSameVersion(response.version())) {
            throw new RemoteFileChangedException(url, version + " -> " + response.version());
          } else if (response.offset() != offset + chunkStart || response.bytes().length != chunkLength) {
            throw new IllegalStateException("Requested " + chunkLength + " bytes at " + (offset + chunkStart) +
              " of " + url + " but got " + response.bytes().length + " at " + response.offset());
          }
          countResponse(response);
          System.arraycopy(response.bytes(), 0, result, chunkStart, chunkLength);
        }));
    }
    var all = CompletableFuture.allOf(chunks.toArray(CompletableFuture[]::new));
    return () -> {
      CompletableFutures.awaitIO(all);
      return result;
    };
  }

  private void countResponse(RangeResponse response) {
    bytesFetched.add(response.bytes().length);
    requestsMade.increment();
  }

  // these mirror readLater: a read that starts in the tail fetches nothing, any other read fetches all of it
  @Override
  public long bytesToFetch(long offset, int length) {
    return offset >= tailOffset ? 0 : length;
  }

  @Override
  public long requestsToFetch(long offset, int length) {
    return offset >= tailOffset ? 0 : Math.ceilDiv(length, chunkBytes);
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
