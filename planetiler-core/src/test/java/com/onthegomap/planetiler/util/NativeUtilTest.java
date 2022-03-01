package com.onthegomap.planetiler.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class NativeUtilTest {

  @Test
  public void testMadvise(@TempDir Path dir) throws IOException {
    String data = "test";
    int bytes = data.getBytes(StandardCharsets.UTF_8).length;
    var path = dir.resolve("file");
    Files.writeString(path, data, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
    try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
      MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, bytes);
      NativeUtil.madvise(buffer, NativeUtil.Madvice.RANDOM);
      byte[] received = new byte[bytes];
      buffer.get(received);
      assertEquals(data, new String(received, StandardCharsets.UTF_8));
    }
  }
}
