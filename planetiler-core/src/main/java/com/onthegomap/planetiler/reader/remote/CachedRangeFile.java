package com.onthegomap.planetiler.reader.remote;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import com.onthegomap.planetiler.util.CompletableFutures;
import com.onthegomap.planetiler.util.Downloader;
import com.onthegomap.planetiler.util.Downloader.ByteRange;
import com.onthegomap.planetiler.util.Downloader.RemoteVersion;
import com.onthegomap.planetiler.util.FileUtils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link RangeFile} that reads a remote file with HTTP range requests and keeps the bytes it fetched in a local
 * sparse file, so later runs read them from disk.
 * <p>
 * The cache is the file at {@code path} plus two small files next to it: {@code path.meta} with the version of the
 * remote file (length, ETag, Last-Modified) and {@code path.ranges} listing the byte ranges fetched so far. Once every
 * byte has been fetched the {@code .ranges} file is deleted, leaving a complete copy of the remote file at
 * {@code path}; {@code .meta} stays so a later refresh can tell whether the remote file changed.
 */
public class CachedRangeFile implements RangeFile {

  private static final Logger LOGGER = LoggerFactory.getLogger(CachedRangeFile.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  // Fetched bytes must be on disk before their range gets recorded, or a crash could leave a range of zeros marked as
  // fetched. Syncing once per this many bytes instead of once per range keeps that cheap; a crash only loses the
  // ranges recorded since the last sync, which get fetched again.
  private static final long SYNC_BYTES = 64L << 20;
  private final Path path;
  private final Path rangesPath;
  private final HttpRangeFile remote;
  // can be set later, for example after a full download of a file with no content-length
  private long length;
  private final FileChannel channel;
  private final RangeSet<Long> cachedByteRanges = TreeRangeSet.create();
  private final RangeSet<Long> rangesToSync = TreeRangeSet.create();
  private final Object rangeLock = new Object();
  private long unsyncedBytes = 0;
  private final AtomicBoolean syncing = new AtomicBoolean(false);
  private FileChannel rangesFile;

  private CachedRangeFile(Path path, HttpRangeFile remote, long length, List<Range<Long>> ranges, boolean newFile)
    throws IOException {
    this.path = path;
    this.rangesPath = rangesPath(path);
    this.remote = remote;
    this.length = length;
    cachedByteRanges.addAll(ranges);
    // SPARSE only matters on Windows (other platforms leave holes in files written at far offsets anyway), where the JDK
    // only applies it together with CREATE_NEW; a file created sparse stays sparse when reopened
    this.channel = newFile ?
      FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.SPARSE, StandardOpenOption.READ,
        StandardOpenOption.WRITE) :
      FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
    this.rangesFile = FileChannel.open(rangesPath, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
  }

  /** Returns the file that lists the cached ranges of a partially downloaded file at {@code path}. */
  public static Path rangesPath(Path path) {
    return path.resolveSibling(path.getFileName() + ".ranges");
  }

  /** Returns the file that stores the version of the remote file cached at {@code path}. */
  public static Path metaPath(Path path) {
    return path.resolveSibling(path.getFileName() + ".meta");
  }

  /** Returns true if {@code path} is a partially downloaded file from a previous {@link CachedRangeFile}. */
  public static boolean isPartial(Path path) {
    return Files.exists(rangesPath(path));
  }

  /**
   * Opens a cache of the remote file at {@code url} at {@code path}, reusing what a previous run fetched if the remote
   * file has not changed since then, or starting over if it has. A complete file at {@code path} gets replaced.
   * Starting over fetches the last {@code tailBytes} bytes along the way (see
   * {@link HttpRangeFile#open(Downloader, String, int)}).
   *
   * @throws IOException                            if the cache can't be read or created, or the remote file can't be
   *                                                opened
   * @throws Downloader.RangesNotSupportedException if the server does not support range requests
   */
  public static CachedRangeFile open(Path path, String url, int tailBytes, Downloader downloader) throws IOException {
    if (!isPartial(path)) {
      return createNew(path, HttpRangeFile.open(downloader, url, tailBytes));
    }
    // reusing a partial cache only needs the remote file's version, the tail is in the cache already
    var remote = HttpRangeFile.open(downloader, url); // NOSONAR
    var previous = readMeta(path);
    if (previous != null && Files.exists(path) && remote.isSameVersion(previous)) {
      return new CachedRangeFile(path, remote, previous.length(), readRanges(path), false);
    }
    LOGGER.info("{} changed since it was partially downloaded to {}, starting over", url, path);
    return createNew(path, remote);
  }

  /**
   * Returns true if {@code path} is a complete copy of the remote file at {@code url} and, with {@code refresh}, of its
   * current version (comparing the size of the file when it has no version from a previous run).
   *
   * @throws IOException if {@code refresh} and the remote file can't be opened
   */
  public static boolean isCompleteCopy(Path path, String url, boolean refresh, Downloader downloader)
    throws IOException {
    return Files.exists(path) && !isPartial(path) && (!refresh || isUpToDate(path, url, downloader));
  }

  private static boolean isUpToDate(Path path, String url, Downloader downloader) throws IOException {
    var meta = readMeta(path);
    var local = meta != null ? meta : new RemoteVersion(Files.size(path), null, null);
    try (var rangeFile = HttpRangeFile.open(downloader, url)) {
      return rangeFile.isSameVersion(local);
    } catch (Downloader.RangesNotSupportedException e) {
      return downloader.isSameVersion(local, e.version());
    }
  }

  private static CachedRangeFile createNew(Path path, HttpRangeFile remote) throws IOException {
    var result = createNew(path, remote, remote.version());
    // keep the bytes that came with the request that opened the file so they don't get fetched again
    var prefetched = remote.prefetched();
    result.write(prefetched.offset(), prefetched.bytes(), 0, prefetched.bytes().length);
    return result;
  }

  /**
   * Creates an empty cache at {@code path} of {@code version} of a remote file, replacing what was there.
   *
   * @throws IOException if the cache can't be created
   */
  public static CachedRangeFile create(Path path, RemoteVersion version) throws IOException {
    return createNew(path, null, version);
  }

  private static CachedRangeFile createNew(Path path, HttpRangeFile remote, RemoteVersion version) throws IOException {
    FileUtils.delete(path, rangesPath(path), metaPath(path));
    FileUtils.createParentDirectories(path);
    // the ranges file marks the cache as partial, so it gets created before any data is written
    Files.writeString(rangesPath(path), "");
    MAPPER.writeValue(metaPath(path).toFile(), version);
    return new CachedRangeFile(path, remote, version.length(), List.of(), true);
  }

  /**
   * Opens a cache at {@code path} left by a previous run without connecting to the remote server, so only the ranges
   * fetched before can be read.
   *
   * @throws IOException if {@code path} is not a partially downloaded file
   */
  public static CachedRangeFile openOffline(Path path) throws IOException {
    var previous = readMeta(path);
    if (previous == null || !isPartial(path)) {
      throw new IOException(path + " is not a partially downloaded file");
    }
    return new CachedRangeFile(path, null, previous.length(), readRanges(path), false);
  }

  private static RemoteVersion readMeta(Path path) {
    try {
      return MAPPER.readValue(metaPath(path).toFile(), RemoteVersion.class);
    } catch (IOException _) {
      return null;
    }
  }

  private static List<Range<Long>> readRanges(Path path) throws IOException {
    List<String> lines = Files.readAllLines(rangesPath(path), StandardCharsets.UTF_8);
    List<Range<Long>> ranges = new ArrayList<>();
    for (String line : lines) {
      String[] parts = line.split(" ");
      // a line cut short by a crash while it was being written can only under-report a range, which is safe
      if (parts.length == 2) {
        long start = Long.parseLong(parts[0]);
        ranges.add(Range.closedOpen(start, start + Long.parseLong(parts[1])));
      }
    }
    return ranges;
  }

  /** Returns the URL of the remote file after following redirects. */
  public String url() {
    return remote.url();
  }

  /** Returns the number of bytes of the file not fetched yet. */
  public long bytesMissing() {
    return missingRanges().stream().mapToLong(ByteRange::length).sum();
  }

  /**
   * Fetches every byte not fetched yet, telling {@code progress} how many bytes arrived as they do, after which
   * {@code path} is a complete copy of the remote file.
   *
   * @throws IOException if a request fails after all retries or the file can't be written
   */
  public void fetchMissing(LongConsumer progress) throws IOException {
    CompletableFutures.awaitIO(remote.fetch(missingRanges(), (position, buffer, offset, length) -> {
      write(position, buffer, offset, length);
      progress.accept(length);
    }));
  }

  private List<ByteRange> missingRanges() {
    RangeSet<Long> missing = TreeRangeSet.create(List.of(Range.closedOpen(0L, length)));
    synchronized (rangeLock) {
      missing.removeAll(cachedByteRanges);
    }
    return missing.asRanges().stream()
      .map(range -> new ByteRange(range.lowerEndpoint(), range.upperEndpoint() - range.lowerEndpoint()))
      .toList();
  }

  // offline (no remote) nothing gets fetched: reading missing bytes fails instead
  @Override
  public long bytesToFetch(long offset, int length) {
    return remote == null || isCached(offset, length) ? 0 : remote.bytesToFetch(offset, length);
  }

  @Override
  public long requestsToFetch(long offset, int length) {
    return remote == null || isCached(offset, length) ? 0 : remote.requestsToFetch(offset, length);
  }

  @Override
  public long bytesFetched() {
    return remote == null ? 0 : remote.bytesFetched();
  }

  @Override
  public long requestsMade() {
    return remote == null ? 0 : remote.requestsMade();
  }

  @Override
  public long length() {
    return length;
  }

  @Override
  public byte[] read(long offset, int length) throws IOException {
    return readLater(offset, length).get();
  }

  @Override
  public Pending readLater(long offset, int length) {
    if (isCached(offset, length)) {
      return () -> FileUtils.readFully(channel, offset, length);
    } else if (remote == null) {
      return () -> {
        throw new IOException(
          path + " is missing bytes " + offset + "-" + (offset + length) + ", run with --download to fetch them");
      };
    }
    Pending fromRemote = remote.readLater(offset, length);
    return () -> {
      byte[] result = fromRemote.get();
      write(offset, result, 0, result.length);
      return result;
    };
  }

  private boolean isCached(long offset, int length) {
    synchronized (rangeLock) {
      return cachedByteRanges.encloses(Range.closedOpen(offset, offset + length));
    }
  }

  /**
   * Stores {@code count} bytes of {@code bytes} from {@code offset}, which are at {@code position} in the remote file.
   * Safe to call from several threads at once for different bytes.
   *
   * @throws IOException if the bytes can't be written
   */
  public void write(long position, byte[] bytes, int offset, int count) throws IOException {
    var buffer = ByteBuffer.wrap(bytes, offset, count);
    while (buffer.hasRemaining()) {
      channel.write(buffer, position + buffer.position() - offset);
    }
    boolean needsSync;
    synchronized (rangeLock) {
      if (rangesFile == null) {
        return;
      }
      var range = Range.closedOpen(position, position + count);
      cachedByteRanges.add(range);
      rangesToSync.add(range);
      unsyncedBytes += count;
      if (finishIfComplete()) {
        return;
      }
      needsSync = unsyncedBytes >= SYNC_BYTES;
    }
    // Syncing can take seconds while a download writes GB/s, so other requests keep writing meanwhile instead of
    // waiting on the lock, and skip syncing while another one does: the next sync records their ranges.
    if (needsSync && syncing.compareAndSet(false, true)) {
      try {
        sync();
      } finally {
        syncing.set(false);
      }
    }
  }

  /**
   * Sets the length of a cache {@link #create created} without one to {@code length}, after every byte of the remote
   * file has been written, which makes {@code path} a complete copy of it.
   *
   * @throws IOException if the cache can't be updated
   */
  public void setLength(long length) throws IOException {
    synchronized (rangeLock) {
      var version = MAPPER.readValue(metaPath(path).toFile(), RemoteVersion.class);
      MAPPER.writeValue(metaPath(path).toFile(),
        new RemoteVersion(length, version.etag(), version.lastModified()));
      this.length = length;
      if (rangesFile != null) {
        finishIfComplete();
      }
    }
  }

  // once every byte is here the file is a complete copy of the remote file; the caller holds the lock
  private boolean finishIfComplete() throws IOException {
    // an unknown length (< 0) waits for setLength
    if (length < 0 || (length > 0 && !cachedByteRanges.encloses(Range.closedOpen(0L, length)))) {
      return false;
    }
    channel.force(false);
    rangesFile.close();
    rangesFile = null;
    Files.delete(rangesPath);
    return true;
  }

  private void sync() throws IOException {
    StringBuilder lines = new StringBuilder();
    synchronized (rangeLock) {
      if (rangesFile == null || rangesToSync.isEmpty()) {
        return;
      }
      for (var range : rangesToSync.asRanges()) {
        lines.append(range.lowerEndpoint()).append(' ').append(range.upperEndpoint() - range.lowerEndpoint())
          .append('\n');
      }
      rangesToSync.clear();
      unsyncedBytes = 0;
    }
    // the bytes of these ranges were written before they got added, so this puts all of them on disk
    channel.force(false);
    synchronized (rangeLock) {
      if (rangesFile != null) {
        rangesFile.write(ByteBuffer.wrap(lines.toString().getBytes(StandardCharsets.UTF_8)));
      }
    }
  }

  @Override
  public void close() throws IOException {
    sync();
    synchronized (rangeLock) {
      // an empty file never gets a write that would finish it
      if (rangesFile != null) {
        finishIfComplete();
      }
      channel.close();
      if (rangesFile != null) {
        rangesFile.close();
      }
    }
  }
}
