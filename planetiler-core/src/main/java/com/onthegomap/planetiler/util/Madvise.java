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
import java.nio.ByteBuffer;
import jnr.ffi.LibraryLoader;
import jnr.ffi.types.size_t;

/**
 * Wrapper for native madvise function to be used via the public API {@link MmapUtil#madvise(ByteBuffer,
 * MmapUtil.Madvice)}.
 * <p>
 * Ported from <a href="https://github.com/upserve/uppend/blob/70967c6f24d7f1a3bbc18799f485d981da93f53b/src/main/java/com/upserve/uppend/blobs/NativeIO.java">upserve/uppend/NativeIO</a>.
 *
 * @see <a href="https://man7.org/linux/man-pages/man2/madvise.2.html">madvise(2) — Linux manual page</a>
 */
class Madvise {

  private static final NativeC nativeC = LibraryLoader.create(NativeC.class).load("c");
  static int pageSize;

  static {
    try {
      pageSize = nativeC.getpagesize(); // 4096 on most Linux
    } catch (UnsatisfiedLinkError e) {
      pageSize = -1;
    }
  }

  private static long alignedAddress(long address) {
    return address & (-pageSize);
  }

  private static long alignedSize(long address, int capacity) {
    long end = address + capacity;
    end = (end + pageSize - 1) & (-pageSize);
    return end - alignedAddress(address);
  }

  /**
   * Give a hint to the system how a mapped memory segment will be used so the OS can optimize performance.
   *
   * @param buffer The mapped memory segment.
   * @param value  The advice to use.
   * @throws IOException If an error occurs or madvise not available on this system
   * @see <a href="https://man7.org/linux/man-pages/man2/madvise.2.html">madvise(2) — Linux manual page</a>
   */
  static void madvise(ByteBuffer buffer, int value) throws IOException {
    if (pageSize <= 0) {
      throw new IOException("madvise failed, pagesize not available");
    }
    final long address = MemoryIO.getInstance().getDirectBufferAddress(buffer);
    final int capacity = buffer.capacity();

    long alignedAddress = alignedAddress(address);
    long alignedSize = alignedSize(alignedAddress, capacity);
    try {
      int val = nativeC.madvise(alignedAddress, alignedSize, value);
      if (val != 0) {
        throw new IOException(String.format("System call madvise failed with code: %d", val));
      }
    } catch (UnsatisfiedLinkError error) {
      throw new IOException("madvise failed", error);
    }
  }

  /** JNR-FFI will automatically compile these to wrappers around native functions with the same signatures. */
  public interface NativeC {

    int madvise(@size_t long address, @size_t long size, int advice);

    int getpagesize();
  }
}
