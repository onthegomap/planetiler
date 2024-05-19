package com.onthegomap.planetiler.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.onthegomap.planetiler.TestUtils;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

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

  @Test
  void testSafeCopy() throws IOException {
    var dest = tmpDir.resolve("unzipped");
    String input = "a1".repeat(1200);
    FileUtils.safeCopy(new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8)), dest);
    assertEquals(input, Files.readString(dest));
  }

  @Test
  void testWalkPathWithPatternDirectory() throws IOException {
    Path parent = tmpDir.resolve(Path.of("a", "b", "c"));
    FileUtils.createDirectory(parent);

    List<Path> txtFiles = Stream.of("1.txt", "2.txt").map(parent::resolve).toList();

    for (var file : txtFiles) {
      Files.write(file, new byte[]{});
    }

    Files.write(parent.resolve("something-that-doesnt-match.blah"), new byte[]{});

    var matchingPaths = FileUtils.walkPathWithPattern(parent, "*.txt");

    assertEquals(
      txtFiles.stream().sorted().toList(),
      matchingPaths.stream().sorted().toList()
    );

    matchingPaths = FileUtils.walkPathWithPattern(parent.resolve("*.txt"));

    assertEquals(
      txtFiles.stream().sorted().toList(),
      matchingPaths.stream().sorted().toList()
    );
  }

  @Test
  void testWalkPathWithPatternDirectoryZip() throws IOException {
    Path parent = tmpDir.resolve(Path.of("a", "b", "c"));
    FileUtils.createDirectory(parent);

    Path zipFile = parent.resolve("fake-zip-file.zip");

    Files.write(zipFile, new byte[]{});
    Files.write(parent.resolve("something-that-doesnt-match.blah"), new byte[]{});

    Function<Path, List<Path>> mockWalkZipFile = zipPath -> List.of(zipPath.resolve("inner.txt"));

    // When we don't provide a callback to recurse into zip files, the path to the zip
    // itself should be returned.
    assertEquals(List.of(zipFile), FileUtils.walkPathWithPattern(parent, "*.zip"));

    // Otherwise, the files inside the zip should be returned.
    assertEquals(List.of(zipFile.resolve("inner.txt")),
      FileUtils.walkPathWithPattern(parent, "*.zip", mockWalkZipFile));


    assertEquals(List.of(zipFile), FileUtils.walkPathWithPattern(parent.resolve("*.zip")));
  }

  @Test
  void testWalkPathWithPatternSingleZip() {
    Path zipPath = TestUtils.pathToResource("shapefile.zip");

    var matchingPaths = FileUtils.walkPathWithPattern(zipPath, "stations.sh[px]");

    assertEquals(
      List.of("/shapefile/stations.shp", "/shapefile/stations.shx"),
      matchingPaths.stream().map(Path::toString).sorted().toList());

    matchingPaths = FileUtils.walkPathWithPattern(zipPath.resolve("stations.sh[px]"));

    assertEquals(
      List.of("/shapefile/stations.shp", "/shapefile/stations.shx"),
      matchingPaths.stream().map(Path::toString).sorted().toList());
  }

  @Test
  void testExpandFile() throws IOException {
    Path path = tmpDir.resolve("toExpand");
    FileUtils.setLength(path, 1000);
    assertEquals(1000, Files.size(path));
  }

  @ParameterizedTest
  @CsvSource(value = {
    "a/b/c; a/b/c;",
    "a/b/*; a/b; *",
    "a/*/b; a; */b",
    "*/b/*; ; */b/*",
    "/*/test; /; */test",
    "a/b={c,d}/other; a; b={c,d}/other",
    "./a/b=?/other; ./a; b=?/other",
  }, delimiter = ';')
  void testParsePathWithPattern(String input, String base, String pattern) {
    var separator = FileSystems.getDefault().getSeparator();
    input = input.replace("/", separator);
    base = base == null ? "" : base.replace("/", separator);
    pattern = pattern == null ? null : pattern.replace("/", separator);
    assertEquals(
      new FileUtils.BaseWithPattern(
        Path.of(base),
        pattern
      ),
      FileUtils.parsePattern(Path.of(input))
    );
  }

  @Test
  void testWalkPathWithPattern() throws IOException {
    var path = tmpDir.resolve("a").resolve("b").resolve("c.txt");
    FileUtils.createParentDirectories(path);
    Files.writeString(path, "test");
    assertEquals(List.of(path), FileUtils.walkPathWithPattern(tmpDir.resolve(Path.of("a", "*", "c.txt"))));
    assertEquals(List.of(path), FileUtils.walkPathWithPattern(tmpDir.resolve(Path.of("*", "*", "c.txt"))));
    assertEquals(List.of(path), FileUtils.walkPathWithPattern(tmpDir.resolve(Path.of("a", "b", "c.txt"))));
  }
}
