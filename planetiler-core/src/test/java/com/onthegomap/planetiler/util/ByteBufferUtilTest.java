package com.onthegomap.planetiler.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Locale;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class ByteBufferUtilTest {

  @Test
  public void testMadviseAndUnmap(@TempDir Path dir) throws IOException {
    String osName = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
    String data = "test";
    int bytes = data.getBytes(StandardCharsets.UTF_8).length;
    var path = dir.resolve("file");
    Files.writeString(path, data, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
    try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
      MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, bytes);
      try {
        ByteBufferUtil.posixMadvise(buffer, ByteBufferUtil.Madvice.RANDOM);
        byte[] received = new byte[bytes];
        buffer.get(received);
        assertEquals(data, new String(received, StandardCharsets.UTF_8));
      } catch (IOException e) {
        if (osName.startsWith("mac") || osName.startsWith("linux")) {
          throw e;
        } else {
          System.out.println("madvise failed, but the system may not support it");
        }
      } finally {
        ByteBufferUtil.free(buffer);
      }
    } finally {
      Files.delete(path);
    }
  }

  @Test
  public void testFreeDirectByteBuffer() throws IOException {
    ByteBufferUtil.free(ByteBuffer.allocateDirect(1));
  }

  @Test
  public void testFreeHeapByteBuffer() throws IOException {
    ByteBufferUtil.free(ByteBuffer.allocate(1));
  }

  private String readString(MappedByteBuffer buffer) {
    byte[] result = new byte[buffer.limit()];
    buffer.get(result);
    return new String(result, StandardCharsets.UTF_8);
  }

  @Test
  public void testMapFile(@TempDir Path dir) throws IOException {
    String data = "test";
    var path = dir.resolve("file");
    Files.writeString(path, data, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
    var channel = FileChannel.open(path, StandardOpenOption.READ);
    MappedByteBuffer[] buffers = ByteBufferUtil.mapFile(channel, 4, 2, true);
    assertEquals(2, buffers.length);

    assertEquals("te", readString(buffers[0]));
    assertEquals("st", readString(buffers[1]));

    ByteBufferUtil.free(buffers);
  }

  @Test
  public void testMapFileLeftoverSegment(@TempDir Path dir) throws IOException {
    String data = "test!";
    var path = dir.resolve("file");
    Files.writeString(path, data, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
    var channel = FileChannel.open(path, StandardOpenOption.READ);
    MappedByteBuffer[] buffers = ByteBufferUtil.mapFile(channel, 5, 2, true);
    assertEquals(3, buffers.length);

    assertEquals("te", readString(buffers[0]));
    assertEquals("st", readString(buffers[1]));
    assertEquals("!", readString(buffers[2]));

    ByteBufferUtil.free(buffers);
  }

  @Test
  public void testMapFileFilterOutSegment(@TempDir Path dir) throws IOException {
    String data = "test!";
    var path = dir.resolve("file");
    Files.writeString(path, data, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
    var channel = FileChannel.open(path, StandardOpenOption.READ);
    MappedByteBuffer[] buffers = ByteBufferUtil.mapFile(channel, 5, 2, true, i -> i != 0);
    assertEquals(3, buffers.length);
    assertNull(buffers[0]);
    assertNotNull(buffers[1]);
    assertNotNull(buffers[2]);

    ByteBufferUtil.free(buffers);
  }
}
