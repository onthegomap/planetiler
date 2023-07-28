package com.onthegomap.planetiler.reader;

import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.util.Downloader;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import org.apache.parquet.io.SeekableInputStream;

public class SeekableHttpInputStream extends SeekableInputStream {

  private final int COPY_BUFFER_SIZE = 8192;
  private final byte[] temp = new byte[COPY_BUFFER_SIZE];
  private final String url;
  private final PlanetilerConfig config;

  private InputStream stream;
  private final long length;
  private long position = 0;

  public SeekableHttpInputStream(String url, PlanetilerConfig config, long length) throws IOException {
    this.url = url;
    this.config = config;
    var conn = Downloader.getUrlConnection(url, config);
    this.length = length;
    this.stream = conn.getInputStream();
  }

  public long getLength() {
    return length;
  }

  public InputStream getStream() {
    return stream;
  }

  @Override
  public void close() throws IOException {
    stream.close();
  }

  @Override
  public long getPos() throws IOException {
    return position;
  }

  @Override
  public void seek(long newPos) throws IOException {
    System.err.println("SEEKING TO " + newPos + " " + url);
    stream.close();
    stream = Downloader.openStreamRange(url, config, newPos, length);
    position = newPos;
  }

  @Override
  public int read() throws IOException {
    return stream.read();
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    return stream.read(b, off, len);
  }

  @Override
  public void readFully(byte[] bytes) throws IOException {
    readFully(stream, bytes, 0, bytes.length);
  }

  @Override
  public void readFully(byte[] bytes, int start, int len) throws IOException {
    readFully(stream, bytes, start, len);
  }

  @Override
  public int read(ByteBuffer buf) throws IOException {
    if (buf.hasArray()) {
      return readHeapBuffer(stream, buf);
    } else {
      return readDirectBuffer(stream, buf, temp);
    }
  }

  @Override
  public void readFully(ByteBuffer buf) throws IOException {
    if (buf.hasArray()) {
      readFullyHeapBuffer(stream, buf);
    } else {
      readFullyDirectBuffer(stream, buf, temp);
    }
  }

  // Visible for testing
  static void readFully(InputStream f, byte[] bytes, int start, int len) throws IOException {
    int offset = start;
    int remaining = len;
    while (remaining > 0) {
      int bytesRead = f.read(bytes, offset, remaining);
      if (bytesRead < 0) {
        throw new EOFException(
          "Reached the end of stream with " + remaining + " bytes left to read");
      }

      remaining -= bytesRead;
      offset += bytesRead;
    }
  }

  // Visible for testing
  static int readHeapBuffer(InputStream f, ByteBuffer buf) throws IOException {
    int bytesRead = f.read(buf.array(), buf.arrayOffset() + buf.position(), buf.remaining());
    if (bytesRead < 0) {
      // if this resulted in EOF, don't update position
      return bytesRead;
    } else {
      buf.position(buf.position() + bytesRead);
      return bytesRead;
    }
  }

  // Visible for testing
  static void readFullyHeapBuffer(InputStream f, ByteBuffer buf) throws IOException {
    readFully(f, buf.array(), buf.arrayOffset() + buf.position(), buf.remaining());
    buf.position(buf.limit());
  }

  // Visible for testing
  static int readDirectBuffer(InputStream f, ByteBuffer buf, byte[] temp) throws IOException {
    // copy all the bytes that return immediately, stopping at the first
    // read that doesn't return a full buffer.
    int nextReadLength = Math.min(buf.remaining(), temp.length);
    int totalBytesRead = 0;
    int bytesRead;

    while ((bytesRead = f.read(temp, 0, nextReadLength)) == temp.length) {
      buf.put(temp);
      totalBytesRead += bytesRead;
      nextReadLength = Math.min(buf.remaining(), temp.length);
    }

    if (bytesRead < 0) {
      // return -1 if nothing was read
      return totalBytesRead == 0 ? -1 : totalBytesRead;
    } else {
      // copy the last partial buffer
      buf.put(temp, 0, bytesRead);
      totalBytesRead += bytesRead;
      return totalBytesRead;
    }
  }

  // Visible for testing
  static void readFullyDirectBuffer(InputStream f, ByteBuffer buf, byte[] temp) throws IOException {
    int nextReadLength = Math.min(buf.remaining(), temp.length);
    int bytesRead = 0;

    while (nextReadLength > 0 && (bytesRead = f.read(temp, 0, nextReadLength)) >= 0) {
      buf.put(temp, 0, bytesRead);
      nextReadLength = Math.min(buf.remaining(), temp.length);
    }

    if (bytesRead < 0 && buf.remaining() > 0) {
      throw new EOFException(
        "Reached the end of stream with " + buf.remaining() + " bytes left to read");
    }
  }
}
