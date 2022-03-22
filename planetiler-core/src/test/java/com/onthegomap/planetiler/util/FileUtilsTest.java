package com.onthegomap.planetiler.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class FileUtilsTest {

  @TempDir
  Path tmpDir;

  @Test
  public void testCreateAndDeleteFileInNestedDirectory() throws IOException {
    Path parent = tmpDir.resolve(Path.of("a", "b", "c"));
    Path file = parent.resolve("file.txt");
    FileUtils.createDirectory(parent);
    Files.write(file, new byte[]{1, 2, 3});
    assertEquals(3, FileUtils.size(file));
    assertEquals(3, FileUtils.size(parent));
    assertTrue(FileUtils.hasExtension(file, "txt"));
    assertFalse(FileUtils.hasExtension(file, "png"));

    FileUtils.delete(tmpDir.resolve("a"));
    assertFalse(Files.exists(file));
    assertFalse(Files.exists(parent));
    assertFalse(Files.exists(tmpDir.resolve("a")));
    assertEquals(0, FileUtils.size(file));
    assertEquals(0, FileUtils.size(parent));
    assertEquals(0, FileUtils.size(tmpDir));
  }

  @Test
  public void testGetFileStore() throws IOException {
    var filestore = Files.getFileStore(tmpDir);
    assertEquals(filestore, FileUtils.getFileStore(tmpDir.resolve("nonexistant_file")));
    assertEquals(filestore, FileUtils.getFileStore(tmpDir.resolve("subdir").resolve("nonexistant_file")));
    var nested = tmpDir.resolve("subdir").resolve("nonexistant_file");
    FileUtils.createParentDirectories(nested);
    assertEquals(filestore, FileUtils.getFileStore(nested));
  }
}
