package com.onthegomap.planetiler.reader.remote;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import com.onthegomap.planetiler.util.Downloader;
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
  // reusing a partial cache only needs the remote file's version, the end of the file is in the cache already
  private static final int VALIDATION_BYTES = 1;
  // Fetched bytes must be on disk before their range gets recorded, or a crash could leave a range of zeros marked as
  // fetched. Syncing once per this many bytes instead of once per range keeps that cheap; a crash only loses the
  // ranges recorded since the last sync, which get fetched again.
  private static final long SYNC_BYTES = 64L << 20;
  private final Path path;
  private final Path rangesPath;
  private final HttpRangeFile remote;
  private final long length;
  private final FileChannel channel;
  private final RangeSet<Long> cachedByteRanges = TreeRangeSet.create();
  private final StringBuilder unsynced = new StringBuilder();
  private long unsyncedBytes = 0;
  private FileChannel rangesFile;

  private CachedRangeFile(Path path, HttpRangeFile remote, long length, List<Range<Long>> ranges) throws IOException {
    this.path = path;
    this.rangesPath = rangesPath(path);
    this.remote = remote;
    this.length = length;
    cachedByteRanges.addAll(ranges);
    // SPARSE only matters on Windows; other platforms leave holes in files written at far offsets anyway
    this.channel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.SPARSE,
      StandardOpenOption.READ, StandardOpenOption.WRITE);
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
   * Deletes a partially downloaded file at {@code path} along with its {@code .ranges} and {@code .meta} files.
   *
   * @return true if there was one
   */
  public static boolean deleteIfPartial(Path path) {
    if (!isPartial(path)) {
      return false;
    }
    FileUtils.delete(path, rangesPath(path), metaPath(path));
    return true;
  }

  /**
   * Opens a cache of the remote file at {@code url} at {@code path}, reusing what a previous run fetched if the remote
   * file has not changed since then, or starting over if it has.
   *
   * @throws IOException              if the cache can't be read or created, or the remote file can't be opened
   * @throws IllegalArgumentException if {@code path} is a complete file rather than a partial download
   */
  public static CachedRangeFile open(Path path, String url, Downloader downloader) throws IOException {
    if (!isPartial(path)) {
      if (Files.exists(path)) {
        throw new IllegalArgumentException(path + " is already complete");
      } else {
        return createNew(path, HttpRangeFile.open(downloader, url));
      }
    }
    var remote = HttpRangeFile.open(downloader, url, VALIDATION_BYTES); // NOSONAR
    var previous = readMeta(path);
    if (previous != null && Files.exists(path) && remote.isSameVersion(previous)) {
      return new CachedRangeFile(path, remote, previous.length(), readRanges(path));
    }
    LOGGER.info("{} changed since it was partially downloaded to {}, starting over", url, path);
    return createNew(path, remote);
  }

  /**
   * Returns true if the remote file at {@code url} is still the version cached at {@code path}, comparing the size of
   * the file when it has no version from a previous run.
   *
   * @throws IOException if the remote file can't be opened
   */
  static boolean isUpToDate(Path path, String url, Downloader downloader) throws IOException {
    var meta = readMeta(path);
    var local = meta != null ? meta : new RemoteVersion(Files.size(path), null, null);
    try (var rangeFile = HttpRangeFile.open(downloader, url, VALIDATION_BYTES)) {
      return rangeFile.isSameVersion(local);
    }
  }

  private static CachedRangeFile createNew(Path path, HttpRangeFile remote) throws IOException {
    FileUtils.delete(path, rangesPath(path), metaPath(path));
    FileUtils.createParentDirectories(path);
    // the ranges file marks the cache as partial, so it gets created before any data is written
    Files.writeString(rangesPath(path), "");
    MAPPER.writeValue(metaPath(path).toFile(), remote.version());
    var result = new CachedRangeFile(path, remote, remote.length(), List.of());
    // the end of the file came with the request that opened it, keep it so the next run doesn't need to fetch it
    result.store(remote.tailOffset(), remote.tail());
    return result;
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
    return new CachedRangeFile(path, null, previous.length(), readRanges(path));
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
      return () -> LocalRangeFile.readFully(channel, offset, length);
    } else if (remote == null) {
      return () -> {
        throw new IOException(
          path + " is missing bytes " + offset + "-" + (offset + length) + ", run with --download to fetch them");
      };
    }
    Pending fromRemote = remote.readLater(offset, length);
    return () -> {
      byte[] result = fromRemote.get();
      store(offset, result);
      return result;
    };
  }

  private synchronized boolean isCached(long offset, int length) {
    return cachedByteRanges.encloses(Range.closedOpen(offset, offset + length));
  }

  private void store(long offset, byte[] bytes) throws IOException {
    var buffer = ByteBuffer.wrap(bytes);
    while (buffer.hasRemaining()) {
      channel.write(buffer, offset + buffer.position());
    }
    synchronized (this) {
      if (rangesFile == null) {
        return;
      }
      cachedByteRanges.add(Range.closedOpen(offset, offset + bytes.length));
      unsynced.append(offset).append(' ').append(bytes.length).append('\n');
      unsyncedBytes += bytes.length;
      if (cachedByteRanges.encloses(Range.closedOpen(0L, length))) {
        // every byte is here now: the file is a complete copy of the remote file
        channel.force(false);
        rangesFile.close();
        rangesFile = null;
        Files.delete(rangesPath);
      } else if (unsyncedBytes >= SYNC_BYTES) {
        sync();
      }
    }
  }

  private synchronized void sync() throws IOException {
    if (rangesFile != null && !unsynced.isEmpty()) {
      channel.force(false);
      rangesFile.write(ByteBuffer.wrap(unsynced.toString().getBytes(StandardCharsets.UTF_8)));
      unsynced.setLength(0);
      unsyncedBytes = 0;
    }
  }

  @Override
  public synchronized void close() throws IOException {
    sync();
    channel.close();
    if (rangesFile != null) {
      rangesFile.close();
    }
  }
}
