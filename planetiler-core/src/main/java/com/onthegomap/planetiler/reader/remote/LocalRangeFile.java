package com.onthegomap.planetiler.reader.remote;

import com.onthegomap.planetiler.util.FileUtils;
import java.io.IOException;
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
      return FileUtils.readFully(channel, offset, length);
    }
  }

  @Override
  public void close() {
    // nothing to release
  }

  @Override
  public String toString() {
    return path.toString();
  }
}
