package com.onthegomap.planetiler.reader.remote;

import com.onthegomap.planetiler.util.Downloader;
import com.onthegomap.planetiler.util.FileUtils;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Random access reads from a local file, or a remote one using HTTP range requests.
 * <p>
 * Implementations are thread-safe.
 */
public interface RangeFile extends Closeable {

  /**
   * Opens a source that should be at {@code path} on local disk.
   * <p>
   * Reads a partially downloaded file left by {@link CachedRangeFile} without connecting to the remote server.
   *
   * @throws IOException if {@code path} is a partially downloaded file that can't be read
   */
  static RangeFile openLocal(Path path) throws IOException {
    // if download is disabled and partial file doesn't contain ranges we need, it will fail while reading
    return CachedRangeFile.isPartial(path) ? CachedRangeFile.openOffline(path) : new LocalRangeFile(path);
  }

  /**
   * Opens a source with a local copy at {@code path} and a remote copy at {@code url}: reads it from {@code path} if it
   * is complete, or from {@code url} with range requests according to {@code mode} otherwise.
   * <p>
   * With {@code refresh}, a complete local copy that a previous {@link DownloadMode#CACHED} run fetched gets replaced
   * if the remote file changed since then.
   *
   * @throws IOException if the remote file can't be opened
   */
  static RangeFile open(Path path, String url, DownloadMode mode, boolean refresh, Downloader downloader)
    throws IOException {
    if (!mode.fetchWhileProcessing()) {
      return openLocal(path);
    }
    boolean complete = Files.exists(path) && !CachedRangeFile.isPartial(path);
    if (complete && (!refresh || CachedRangeFile.isUpToDate(path, url, downloader))) {
      return new LocalRangeFile(path);
    } else if (mode == DownloadMode.STREAMING) {
      return HttpRangeFile.open(downloader, url);
    } else if (complete) {
      // the remote file changed, so the outdated complete copy and its version have to go before caching the new one
      FileUtils.delete(path, CachedRangeFile.metaPath(path));
    }
    return CachedRangeFile.open(path, url, downloader);
  }

  /**
   * Returns how many bytes reading the {@code length} bytes at {@code offset} would fetch from the remote file right
   * now; 0 for local files.
   */
  default long bytesToFetch(long offset, int length) {
    return 0;
  }

  /** Returns how many requests reading the {@code length} bytes at {@code offset} would make right now. */
  default long requestsToFetch(long offset, int length) {
    return 0;
  }

  /** Returns the number of bytes fetched from the remote file so far, not counting retries; 0 for local files. */
  default long bytesFetched() {
    return 0;
  }

  /** Returns the number of requests to the remote file that succeeded so far, not counting retries. */
  default long requestsMade() {
    return 0;
  }

  /** Returns the size of the file in bytes. */
  long length() throws IOException;

  /**
   * Returns {@code length} bytes starting at {@code offset}.
   *
   * @throws IOException if the bytes can't be read
   */
  byte[] read(long offset, int length) throws IOException;

  /**
   * Returns a handle to read {@code length} bytes starting at {@code offset} later.
   * <p>
   * Remote implementations start fetching the bytes right away so that {@link Pending#get()} blocks less, local
   * implementations read them when {@link Pending#get()} is called.
   */
  default Pending readLater(long offset, int length) {
    return () -> read(offset, length);
  }

  /** Bytes requested by {@link #readLater(long, int)}. */
  @FunctionalInterface
  interface Pending {

    /** Returns the bytes, waiting for them to arrive if necessary. */
    byte[] get() throws IOException;
  }
}
