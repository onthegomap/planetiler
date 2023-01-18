/*
Copyright 2014 The Bazel Authors. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

/**
 * Encode and decode Protocol Buffer-style VarInts.
 * Adapted from <a href="https://github.com/bazelbuild/bazel/blob/master/src/main/java/com/google/devtools/build/lib/util/VarInt.java">Bazel</a>,
 * with only the methods related to 64-bit Longs.
 */
package com.onthegomap.planetiler.util;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class VarInt {
  private VarInt() {}

  public static int varLongSize(long v) {
    int result = 0;
    do {
      result++;
      v >>>= 7;
    } while (v != 0);
    return result;
  }

  /**
   * Reads an up to 64 bit long varint from the current position of the given ByteBuffer and returns the decoded value
   * as long.
   *
   * <p>
   * The position of the buffer is advanced to the first byte after the decoded varint.
   *
   * @param src the ByteBuffer to get the var int from
   * @return The integer value of the decoded long varint
   */
  public static long getVarLong(ByteBuffer src) {
    long tmp;
    if ((tmp = src.get()) >= 0) {
      return tmp;
    }
    long result = tmp & 0x7f;
    if ((tmp = src.get()) >= 0) {
      result |= tmp << 7;
    } else {
      result |= (tmp & 0x7f) << 7;
      if ((tmp = src.get()) >= 0) {
        result |= tmp << 14;
      } else {
        result |= (tmp & 0x7f) << 14;
        if ((tmp = src.get()) >= 0) {
          result |= tmp << 21;
        } else {
          result |= (tmp & 0x7f) << 21;
          if ((tmp = src.get()) >= 0) {
            result |= tmp << 28;
          } else {
            result |= (tmp & 0x7f) << 28;
            if ((tmp = src.get()) >= 0) {
              result |= tmp << 35;
            } else {
              result |= (tmp & 0x7f) << 35;
              if ((tmp = src.get()) >= 0) {
                result |= tmp << 42;
              } else {
                result |= (tmp & 0x7f) << 42;
                if ((tmp = src.get()) >= 0) {
                  result |= tmp << 49;
                } else {
                  result |= (tmp & 0x7f) << 49;
                  if ((tmp = src.get()) >= 0) {
                    result |= tmp << 56;
                  } else {
                    result |= (tmp & 0x7f) << 56;
                    result |= ((long) src.get()) << 63;
                  }
                }
              }
            }
          }
        }
      }
    }
    return result;
  }

  public static void putVarLong(long v, ByteBuffer sink) {
    while (true) {
      int bits = ((int) v) & 0x7f;
      v >>>= 7;
      if (v == 0) {
        sink.put((byte) bits);
        return;
      }
      sink.put((byte) (bits | 0x80));
    }
  }

  /**
   * Encodes a long integer in a variable-length encoding, 7 bits per byte.
   * 
   * @param v            the value to encode
   * @param outputStream the OutputStream to add the encoded value
   */
  public static void putVarLong(long v, OutputStream outputStream) throws IOException {
    byte[] bytes = new byte[varLongSize(v)];
    ByteBuffer sink = ByteBuffer.wrap(bytes);
    putVarLong(v, sink);
    outputStream.write(bytes);
  }
}
