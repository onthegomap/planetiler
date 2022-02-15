/*
MIT License

Copyright (c) 2017 Upserve, Inc.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
 */
package com.onthegomap.planetiler.util;

import com.kenai.jffi.MemoryIO;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import jnr.ffi.LibraryLoader;
import jnr.ffi.types.size_t;

/**
 * Adapted from https://github.com/upserve/uppend/blob/master/src/main/java/com/upserve/uppend/blobs/NativeIO.java and
 * simplified to only set random.
 */
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
