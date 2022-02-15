package com.onthegomap.planetiler.util;

import com.kenai.jffi.MemoryIO;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import jnr.ffi.LibraryLoader;
import jnr.ffi.types.size_t;

public class Madvise {

  public interface NativeC {

    int madvise(@size_t long address, @size_t long size, int advice);

    int getpagesize();
  }

  private static final NativeC nativeC = LibraryLoader.create(NativeC.class).load("c");
  public static final int pageSize = nativeC.getpagesize(); // 4096 on most Linux

  static long alignedAddress(long address) {
    return address & (-pageSize);
  }

  static long alignedSize(long address, int capacity) {
    long end = address + capacity;
    end = (end + pageSize - 1) & (-pageSize);
    return end - alignedAddress(address);
  }

  public static void random(MappedByteBuffer buffer) throws IOException {
    final long address = MemoryIO.getInstance().getDirectBufferAddress(buffer);
    final int capacity = buffer.capacity();

    long alignedAddress = alignedAddress(address);
    long alignedSize = alignedSize(alignedAddress, capacity);
    int val = nativeC.madvise(alignedAddress, alignedSize, 1 /* RANDOM */);

    if (val != 0) {
      throw new IOException(String.format("System call madvise failed with code: %d", val));
    }
  }
}
