package com.onthegomap.planetiler.reader.remote;

import com.onthegomap.planetiler.util.Downloader;
import com.onthegomap.planetiler.util.Downloader.ByteRange;
import com.onthegomap.planetiler.util.RequestPlanner;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

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
   * is complete, or from {@code url} with range requests according to {@code mode} otherwise, fetching its last
   * {@code tailBytes} bytes with the request that opens it (see {@link HttpRangeFile#open(Downloader, String, int)}).
   * <p>
   * With {@code refresh}, a complete local copy that a previous {@link DownloadMode#CACHED} run fetched gets replaced
   * if the remote file changed since then. If the server does not support range requests, the whole file gets
   * downloaded to {@code path} first.
   *
   * @throws IOException if the remote file can't be opened
   */
  static RangeFile open(Path path, String url, int tailBytes, DownloadMode mode, boolean refresh,
    Downloader downloader) throws IOException {
    if (!mode.fetchWhileProcessing()) {
      return openLocal(path);
    }
    try {
      if (CachedRangeFile.isCompleteCopy(path, url, refresh, downloader)) {
        return new LocalRangeFile(path);
      }
      return mode == DownloadMode.STREAMING ? HttpRangeFile.open(downloader, url, tailBytes) :
        CachedRangeFile.open(path, url, tailBytes, downloader);
    } catch (Downloader.RangesNotSupportedException _) {
      downloader.downloadWhole(url, path);
      return new LocalRangeFile(path);
    }
  }

  /**
   * Returns true if reading the {@code length} bytes at {@code offset} would fetch them from the remote file or false
   * for local files and bytes cached already.
   */
  default boolean needsFetch(long offset, long length) {
    return false;
  }

  /**
   * Returns the requests {@link #readLater(List)} would make right now to read {@code ranges}, or empty list for local
   * files.
   */
  default RequestPlanner.Plan planFetch(List<ByteRange> ranges) {
    return RequestPlanner.Plan.EMPTY;
  }

  /** Returns the number of bytes fetched from the remote file so far, not counting retries; 0 for local files. */
  default long bytesFetched() {
    return 0;
  }

  /** Returns the number of requests made to the remote file so far, not counting retries. */
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
   * Returns a handle per range in {@code ranges} (which must be in ascending order and must not overlap) to read its
   * bytes later.
   * <p>
   * Remote implementations fetch the ranges with the requests {@link RequestPlanner} plans for them, which can span
   * several ranges. A request gets sent once the first range it covers {@link Pending#start() starts}, and its bytes
   * get read off the network only as far as the ranges that started, so callers start ranges in order shortly before
   * they need them. Local implementations read the bytes when {@link Pending#get()} is called.
   */
  default List<Pending> readLater(List<ByteRange> ranges) {
    return ranges.stream().<Pending>map(range -> () -> read(range.offset(), Math.toIntExact(range.length())))
      .toList();
  }

  /** Bytes requested by {@link #readLater(List)}. */
  @FunctionalInterface
  interface Pending {

    /** Says {@link #get()} will be called soon, so the bytes can start arriving now. */
    default void start() {}

    /** Starts these bytes if necessary, and returns when they have all arrived. */
    byte[] get() throws IOException;
  }
}
