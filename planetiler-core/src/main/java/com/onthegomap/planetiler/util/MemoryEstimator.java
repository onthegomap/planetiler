package com.onthegomap.planetiler.util;

import com.carrotsearch.hppc.Accountable;

/** Utilities to estimate the size of in-memory objects. */
@SuppressWarnings("SameReturnValue")
public class MemoryEstimator {

  public static final long CLASS_HEADER_BYTES = 24L;
  public static final long INT_BYTES = 4L;
  public static final long LONG_BYTES = 8L;
  public static final long POINTER_BYTES = 8L;

  public static long estimateSize(HasEstimate object) {
    return object == null ? 0 : object.estimateMemoryUsageBytes();
  }

  public static long estimateSize(String string) {
    return string == null ? 0 : (54 + string.getBytes().length);
  }

  public static long estimateSizeInt(int ignored) {
    return INT_BYTES;
  }

  public static long estimateSizeLong(long ignored) {
    return LONG_BYTES;
  }

  public static long estimateSize(byte ignored) {
    return 1;
  }

  public static long estimateSize(boolean ignored) {
    return 1;
  }

  /** Estimates the size of an HPPC {@link Accountable} instance. */
  public static long estimateSize(Accountable object) {
    return object == null ? 0 : object.ramBytesAllocated();
  }

  public static long estimateArraySize(int length, long entrySize) {
    return CLASS_HEADER_BYTES + entrySize * length;
  }

  public static long estimateByteArraySize(int length) {
    return estimateArraySize(length, 1);
  }

  public static long estimateIntArraySize(int length) {
    return estimateArraySize(length, INT_BYTES);
  }

  public static long estimateLongArraySize(int length) {
    return estimateArraySize(length, LONG_BYTES);
  }

  public static long estimateObjectArraySize(int length) {
    return estimateArraySize(length, POINTER_BYTES);
  }

  public static long estimateSize(byte[] array) {
    return estimateByteArraySize(array.length);
  }

  public static long estimateSize(int[] array) {
    return estimateIntArraySize(array.length);
  }

  public static long estimateSize(long[] array) {
    return estimateLongArraySize(array.length);
  }

  public static long estimateSize(Object[] array) {
    return estimateObjectArraySize(array.length);
  }

  public interface HasEstimate {

    long estimateMemoryUsageBytes();
  }
}
