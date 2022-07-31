package com.onthegomap.planetiler.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FileUtilsTest {

  @TempDir
  Path tmpDir;

  @Test
  void testCreateAndDeleteFileInNestedDirectory() throws IOException {
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
  void testGetFileStore() throws IOException {
    var filestore = Files.getFileStore(tmpDir);
    assertEquals(filestore, FileUtils.getFileStore(tmpDir.resolve("nonexistant_file")));
    assertEquals(filestore, FileUtils.getFileStore(tmpDir.resolve("subdir").resolve("nonexistant_file")));
    var nested = tmpDir.resolve("subdir").resolve("nonexistant_file");
    FileUtils.createParentDirectories(nested);
    assertEquals(filestore, FileUtils.getFileStore(nested));
  }

  @Test
  void testUnzip() throws IOException {
    var dest = tmpDir.resolve("unzipped");
    FileUtils.unzipResource("/shapefile.zip", dest);
    try (var walkStream = Files.walk(dest)) {
      var all = walkStream.toList();
      var directories = all.stream()
        .filter(Files::isDirectory)
        .map(tmpDir::relativize)
        .collect(Collectors.toSet());
      var files = all.stream()
        .filter(Files::isRegularFile)
        .map(tmpDir::relativize)
        .collect(Collectors.toSet());
      assertEquals(Set.of(
        Path.of("unzipped"),
        Path.of("unzipped", "shapefile")
      ), directories);
      assertEquals(Set.of(
        Path.of("unzipped", "shapefile", "stations.shx"),
        Path.of("unzipped", "shapefile", "stations.cpg"),
        Path.of("unzipped", "shapefile", "stations.shp"),
        Path.of("unzipped", "shapefile", "stations.dbf"),
        Path.of("unzipped", "shapefile", "stations.prj")
      ), files);
    }
    assertEquals(
      """
        GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137,298.257223563]],PRIMEM["Greenwich",0],UNIT["Degree",0.017453292519943295]]
        """
        .strip(),
      Files.readString(dest.resolve("shapefile").resolve("stations.prj"))
    );
    assertEquals(
      "UTF8",
      Files.readString(dest.resolve("shapefile").resolve("stations.cpg"))
    );
  }
}
