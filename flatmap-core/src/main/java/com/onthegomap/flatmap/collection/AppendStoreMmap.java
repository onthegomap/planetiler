package com.onthegomap.flatmap.collection;

import com.onthegomap.flatmap.util.FileUtils;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

abstract class AppendStoreMmap implements AppendStore {

  final DataOutputStream outputStream;
  final int segmentBits;
  final long segmentMask;
  final long segmentBytes;
  private final Path path;
  long outIdx = 0;
  private volatile MappedByteBuffer[] segments;
  private volatile FileChannel channel;

  AppendStoreMmap(Path path) {
    this(path, 1 << 20); // 1MB
  }

  AppendStoreMmap(Path path, long segmentSizeBytes) {
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
    MappedByteBuffer[] result = segments;
    if (result == null) {
      synchronized (this) {
        if ((result = segments) == null) {
          try {
            outputStream.close();
            channel = FileChannel.open(path, StandardOpenOption.READ);
            int segmentCount = (int) (outIdx / segmentBytes + 1);
            result = new MappedByteBuffer[segmentCount];
            int i = 0;
            for (long segmentStart = 0; segmentStart < outIdx; segmentStart += segmentBytes) {
              long segmentEnd = Math.min(segmentBytes, outIdx - segmentStart);
              result[i++] = channel.map(FileChannel.MapMode.READ_ONLY, segmentStart, segmentEnd);
            }
            segments = result;
          } catch (IOException e) {
            throw new IllegalStateException("Failed preparing SequentialWriteRandomReadFile for reads", e);
          }
        }
      }
    }
    return result;
  }

  @Override
  public void close() throws IOException {
    outputStream.close();
    synchronized (this) {
      if (channel != null) {
        channel.close();
      }
      if (segments != null) {
        Arrays.fill(segments, null);
      }
    }
  }

  @Override
  public long bytesOnDisk() {
    return FileUtils.size(path);
  }

  static class Ints extends AppendStoreMmap implements AppendStore.Ints {

    Ints(Path path) {
      super(path);
    }

    Ints(Path path, long segmentSizeBytes) {
      super(path, segmentSizeBytes);
    }

    @Override
    public void writeInt(int value) {
      try {
        outputStream.writeInt(value);
        outIdx += 4;
      } catch (IOException e) {
        throw new IllegalStateException("Error writing int", e);
      }
    }

    @Override
    public int getInt(long index) {
      checkIndex(index);
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

    Longs(Path path) {
      super(path);
    }

    Longs(Path path, long segmentSizeBytes) {
      super(path, segmentSizeBytes);
    }

    @Override
    public void writeLong(long value) {
      try {
        outputStream.writeLong(value);
        outIdx += 8;
      } catch (IOException e) {
        throw new IllegalStateException("Error writing long", e);
      }
    }

    @Override
    public long getLong(long index) {
      checkIndex(index);
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
