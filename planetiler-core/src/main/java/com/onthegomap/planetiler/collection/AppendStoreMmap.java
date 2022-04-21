package com.onthegomap.planetiler.collection;

import com.onthegomap.planetiler.util.ByteBufferUtil;
import com.onthegomap.planetiler.util.FileUtils;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An array of primitives backed by memory-mapped file.
 */
abstract class AppendStoreMmap implements AppendStore {

  private static final Logger LOGGER = LoggerFactory.getLogger(AppendStoreMmap.class);

  // writes are done using a BufferedOutputStream
  final DataOutputStream outputStream;
  final int segmentBits;
  final long segmentMask;
  final long segmentBytes;
  private final Path path;
  private final boolean madvise;
  long outIdx = 0;
  private volatile MappedByteBuffer[] segments;
  private volatile FileChannel channel;

  AppendStoreMmap(Path path, boolean madvise) {
    this(path, 1 << 30, madvise); // 1GB
  }

  AppendStoreMmap(Path path, long segmentSizeBytes, boolean madvise) {
    FileUtils.createParentDirectories(path);
    this.madvise = madvise;
    segmentBits = (int) (Math.log(segmentSizeBytes) / Math.log(2));
    segmentMask = (1L << segmentBits) - 1;
    segmentBytes = segmentSizeBytes;
    if (segmentSizeBytes % 8 != 0 || (1L << segmentBits != segmentSizeBytes)) {
      throw new IllegalArgumentException("segment size must be a multiple of 8 and power of 2: " + segmentSizeBytes);
    }
    this.path = path;
    try {
      this.outputStream = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(path), 50_000));
    } catch (IOException e) {
      throw new IllegalStateException("Could not create SequentialWriteRandomReadFile output stream", e);
    }
  }

  MappedByteBuffer[] getSegments() {
    if (segments == null) {
      synchronized (this) {
        if (segments == null) {
          try {
            // prepare the memory mapped file: stop writing, start reading
            outputStream.close();
            channel = FileChannel.open(path, StandardOpenOption.READ);
            segments = ByteBufferUtil.mapFile(channel, outIdx, segmentBytes, madvise);
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        }
      }
    }
    return segments;
  }

  @Override
  public void close() throws IOException {
    outputStream.close();
    synchronized (this) {
      if (channel != null) {
        channel.close();
      }
      if (segments != null) {
        try {
          ByteBufferUtil.free(segments);
        } catch (IOException e) {
          LOGGER.info("Unable to unmap {} {}", path, e);
        }
        Arrays.fill(segments, null);
      }
    }
  }

  @Override
  public long diskUsageBytes() {
    return FileUtils.size(path);
  }

  static class Ints extends AppendStoreMmap implements AppendStore.Ints {

    Ints(Storage.Params params) {
      this(params.path(), params.madvise());
    }

    Ints(Path path, boolean madvise) {
      super(path, madvise);
    }

    Ints(Path path, long segmentSizeBytes, boolean madvise) {
      super(path, segmentSizeBytes, madvise);
    }

    @Override
    public void appendInt(int value) {
      try {
        outputStream.writeInt(value);
        outIdx += 4;
      } catch (IOException e) {
        throw new IllegalStateException("Error writing int", e);
      }
    }

    @Override
    public int getInt(long index) {
      checkIndexInBounds(index);
      MappedByteBuffer[] segments = getSegments();
      long byteOffset = index << 2;
      int idx = (int) (byteOffset >>> segmentBits);
      int offset = (int) (byteOffset & segmentMask);
      return segments[idx].getInt(offset);
    }

    @Override
    public long size() {
      return outIdx >>> 2;
    }
  }

  static class Longs extends AppendStoreMmap implements AppendStore.Longs {

    Longs(Storage.Params params) {
      this(params.path(), params.madvise());
    }

    Longs(Path path, boolean madvise) {
      super(path, madvise);
    }

    Longs(Path path, long segmentSizeBytes, boolean madvise) {
      super(path, segmentSizeBytes, madvise);
    }

    @Override
    public void appendLong(long value) {
      try {
        outputStream.writeLong(value);
        outIdx += 8;
      } catch (IOException e) {
        throw new IllegalStateException("Error writing long", e);
      }
    }

    @Override
    public long getLong(long index) {
      checkIndexInBounds(index);
      MappedByteBuffer[] segments = getSegments();
      long byteOffset = index << 3;
      int idx = (int) (byteOffset >>> segmentBits);
      int offset = (int) (byteOffset & segmentMask);
      return segments[idx].getLong(offset);
    }

    @Override
    public long size() {
      return outIdx >>> 3;
    }
  }
}
