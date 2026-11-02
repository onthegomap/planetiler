package com.onthegomap.planetiler.reader.parquet;

import com.onthegomap.planetiler.reader.remote.RangeFile;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

/**
 * Adapts a {@link RangeFile} to parquet's {@link InputFile}, serving reads from bytes that were already fetched where
 * possible.
 */
class RangeInputFile implements InputFile {

  private final RangeFile file;
  private final long length;
  private final byte[] prefetched;
  private final long prefetchedOffset;

  /**
   * Reads from {@code file}, using {@code prefetched} for the bytes starting at {@code prefetchedOffset}: a row group
   * fetched ahead of time, so parquet's reads of its column chunks don't each go to the file.
   */
  RangeInputFile(RangeFile file, long length, byte[] prefetched, long prefetchedOffset) {
    this.file = file;
    this.length = length;
    this.prefetched = prefetched;
    this.prefetchedOffset = prefetchedOffset;
  }

  RangeInputFile(RangeFile file, long length) {
    this(file, length, null, 0);
  }

  @Override
  public long getLength() {
    return length;
  }

  @Override
  public SeekableInputStream newStream() {
    return new SeekableInputStream() {
      private long pos = 0;

      @Override
      public long getPos() {
        return pos;
      }

      @Override
      public void seek(long newPos) {
        pos = newPos;
      }

      @Override
      public int read() throws IOException {
        byte[] result = new byte[1];
        return read(result, 0, 1) < 0 ? -1 : (result[0] & 0xff);
      }

      @Override
      public int read(byte[] bytes, int off, int len) throws IOException {
        int toRead = (int) Math.min(len, length - pos);
        if (toRead <= 0) {
          return len == 0 ? 0 : -1;
        }
        readFully(bytes, off, toRead);
        return toRead;
      }

      @Override
      public void readFully(byte[] bytes) throws IOException {
        readFully(bytes, 0, bytes.length);
      }

      @Override
      public void readFully(byte[] bytes, int start, int len) throws IOException {
        if (pos + len > length) {
          throw new EOFException("Unexpected EOF at " + length + " reading " + len + " bytes at " + pos);
        }
        copy(pos, bytes, start, len);
        pos += len;
      }

      @Override
      public int read(ByteBuffer buf) throws IOException {
        int toRead = (int) Math.min(buf.remaining(), length - pos);
        if (toRead <= 0) {
          return buf.hasRemaining() ? -1 : 0;
        }
        readFully(buf.slice(buf.position(), toRead));
        buf.position(buf.position() + toRead);
        return toRead;
      }

      @Override
      public void readFully(ByteBuffer buf) throws IOException {
        int len = buf.remaining();
        if (buf.hasArray()) {
          readFully(buf.array(), buf.arrayOffset() + buf.position(), len);
        } else {
          byte[] bytes = new byte[len];
          readFully(bytes, 0, len);
          buf.put(buf.position(), bytes);
        }
        buf.position(buf.position() + len);
      }
    };
  }

  private void copy(long offset, byte[] dest, int destOffset, int len) throws IOException {
    if (prefetched != null && offset >= prefetchedOffset && offset + len <= prefetchedOffset + prefetched.length) {
      System.arraycopy(prefetched, (int) (offset - prefetchedOffset), dest, destOffset, len);
    } else {
      System.arraycopy(file.read(offset, len), 0, dest, destOffset, len);
    }
  }
}
