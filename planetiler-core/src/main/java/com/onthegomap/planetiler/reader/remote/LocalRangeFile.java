package com.onthegomap.planetiler.reader.remote;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * A {@link RangeFile} that reads from a file on local disk.
 * <p>
 * Opens the file for each read so that it holds no resources between reads and does not need to be closed.
 */
public class LocalRangeFile implements RangeFile {

  private final Path path;

  public LocalRangeFile(Path path) {
    this.path = path;
  }

  @Override
  public long length() throws IOException {
    return Files.size(path);
  }

  @Override
  public byte[] read(long offset, int length) throws IOException {
    try (var channel = FileChannel.open(path, StandardOpenOption.READ)) {
      return readFully(channel, offset, length);
    }
  }

  static byte[] readFully(FileChannel channel, long offset, int length) throws IOException {
    byte[] result = new byte[length];
    var buffer = ByteBuffer.wrap(result);
    while (buffer.hasRemaining()) {
      if (channel.read(buffer, offset + buffer.position()) < 0) {
        throw new EOFException("Unexpected EOF at " + (offset + buffer.position()));
      }
    }
    return result;
  }

  @Override
  public void close() {
    // nothing to release
  }
}
