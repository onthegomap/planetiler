package com.onthegomap.planetiler.util;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.function.IntPredicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utilities for working with memory-mapped and direct byte buffers.
 */
public class ByteBufferUtil {

  private static final Logger LOGGER = LoggerFactory.getLogger(ByteBufferUtil.class);


  /** Attempts to invoke native utility and logs an error message if not available. */
  public static void init() {
    if (Madvise.pageSize < 0) {
      try {
        posixMadvise(ByteBuffer.allocateDirect(1), Madvice.RANDOM);
      } catch (IOException e) {
        LOGGER.info("madvise not available on this system");
      }
    }
  }

  /**
   * Give a hint to the system how a mapped memory segment will be used so the OS can optimize performance.
   *
   * @param buffer The mapped memory segment.
   * @param value  The advice to use.
   * @throws IOException If an error occurs or madvise not available on this system
   * @see <a href="https://man7.org/linux/man-pages/man3/posix_madvise.3.html">posix_madvise(3) — Linux manual page</a>
   */
  public static void posixMadvise(ByteBuffer buffer, Madvice value) throws IOException {
    Madvise.posixMadvise(buffer, value.value);
  }

  /**
   * Attempt to force-unmap a list of memory-mapped file segments, so it can safely be deleted.
   * <p>
   * Can also be used to force-deallocate a direct byte buffer.
   *
   * @param segments The segments to free
   * @throws IOException If any error occurs freeing the segment
   */
  public static void free(ByteBuffer... segments) throws IOException {
    try {
      // attempt to force-unmap the file, so we can delete it later
      // https://stackoverflow.com/questions/2972986/how-to-unmap-a-file-from-memory-mapped-using-filechannel-in-java
      Class<?> unsafeClass;
      try {
        unsafeClass = Class.forName("sun.misc.Unsafe");
      } catch (Exception ex) {
        unsafeClass = Class.forName("jdk.internal.misc.Unsafe");
      }
      Method clean = unsafeClass.getMethod("invokeCleaner", ByteBuffer.class);
      clean.setAccessible(true);
      Field theUnsafeField = unsafeClass.getDeclaredField("theUnsafe");
      theUnsafeField.setAccessible(true);
      Object theUnsafe = theUnsafeField.get(null);
      for (ByteBuffer buffer : segments) {
        if (buffer != null && (buffer instanceof MappedByteBuffer || buffer.isDirect())) {
          clean.invoke(theUnsafe, buffer);
        }
      }
    } catch (Exception e) {
      throw new IOException("Unable to unmap", e);
    }
  }

  /**
   * Same as {@link #mapFile(FileChannel, long, long, boolean, IntPredicate)} except map every segment without testing
   * that it has data first.
   */
  public static MappedByteBuffer[] mapFile(FileChannel readChannel, long expectedLength, long segmentBytes,
    boolean madvise) throws IOException {
    return mapFile(readChannel, expectedLength, segmentBytes, madvise, i -> true);
  }

  /**
   * Memory-map many segments of a file.
   *
   * @param file           A channel for the input file being read
   * @param expectedLength Expected number of bytes in the file
   * @param segmentBytes   Number of bytes in each segment
   * @param madvise        {@code true} to use linux madvise random on the file to improve read performance
   * @param segmentHasData Predicate that returns {@code false} when a segment index has no data
   * @return The array of mapped segments.
   * @throws IOException If an error occurs reading from the file
   */
  public static MappedByteBuffer[] mapFile(FileChannel file, long expectedLength, long segmentBytes,
    boolean madvise, IntPredicate segmentHasData) throws IOException {
    assert expectedLength == file.size();
    int segmentCount = (int) (expectedLength / segmentBytes);
    if (expectedLength % segmentBytes != 0) {
      segmentCount++;
    }
    MappedByteBuffer[] segmentsArray = new MappedByteBuffer[segmentCount];
    int i = 0;
    boolean madviseFailed = false;
    for (long segmentStart = 0; segmentStart < expectedLength; segmentStart += segmentBytes) {
      long segmentLength = Math.min(segmentBytes, expectedLength - segmentStart);
      if (segmentHasData.test(i)) {
        MappedByteBuffer buffer = file.map(FileChannel.MapMode.READ_ONLY, segmentStart, segmentLength);
        if (madvise) {
          try {
            ByteBufferUtil.posixMadvise(buffer, ByteBufferUtil.Madvice.RANDOM);
          } catch (IOException e) {
            if (!madviseFailed) { // log once
              LOGGER.info(
                "madvise not available on this system - node location lookup may be slower when less free RAM is available outside the JVM");
              madviseFailed = true;
            }
          }
        }
        segmentsArray[i] = buffer;
      }
      i++;
    }
    return segmentsArray;
  }

  /** Values from https://man7.org/linux/man-pages/man2/madvise.2.html */
  public enum Madvice {
    NORMAL(0),
    RANDOM(1),
    SEQUENTIAL(2),
    WILLNEED(3),
    DONTNEED(4);

    final int value;

    Madvice(int value) {
      this.value = value;
    }
  }
}
