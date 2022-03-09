package com.onthegomap.planetiler.util;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utilities for working with memory-mapped files.
 */
public class MmapUtil {

  private static final Logger LOGGER = LoggerFactory.getLogger(MmapUtil.class);


  /** Attempts to invoke native utility and logs an error message if not available. */
  public static void init() {
    if (Madvise.pageSize < 0) {
      try {
        madvise(ByteBuffer.allocateDirect(1), Madvice.RANDOM);
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
   * @see <a href="https://man7.org/linux/man-pages/man2/madvise.2.html">madvise(2) — Linux manual page</a>
   */
  public static void madvise(ByteBuffer buffer, Madvice value) throws IOException {
    Madvise.madvise(buffer, value.value);
  }

  /**
   * Attempt to force-unmap a list of memory-mapped file segments so it can safely be deleted.
   *
   * @param segments The segments to unmap
   * @throws IOException If any error occurs unmapping the segment
   */
  public static void unmap(MappedByteBuffer... segments) throws IOException {
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
      for (MappedByteBuffer buffer : segments) {
        if (buffer != null) {
          clean.invoke(theUnsafe, buffer);
        }
      }
    } catch (Exception e) {
      throw new IOException("Unable to unmap", e);
    }
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
