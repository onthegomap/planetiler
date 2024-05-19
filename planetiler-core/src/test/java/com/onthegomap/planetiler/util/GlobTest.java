package com.onthegomap.planetiler.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class GlobTest {
  @TempDir
  Path tmpDir;

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
      new Glob(Path.of(base), pattern),
      Glob.parse(input)
    );
  }

  @Test
  void testWalkPathWithPattern() throws IOException {
    var path = tmpDir.resolve("a").resolve("b").resolve("c.txt");
    FileUtils.createParentDirectories(path);
    Files.writeString(path, "test");
    assertEquals(List.of(path), Glob.of(tmpDir).resolve("a", "*", "c.txt").find());
    System.err.println(Glob.of(tmpDir).resolve("*", "*", "c.txt"));
    System.err.println(Glob.of(tmpDir).resolve("*", "*", "c.txt").find()); // nope
    System.err.println(Glob.of(tmpDir).resolve("**", "c.txt").find()); // ok
    System.err.println(Glob.of(tmpDir).resolve("*", "b", "c.txt").find()); // nada
    assertEquals(List.of(path), Glob.of(tmpDir).resolve("*", "*", "c.txt").find());
    assertEquals(List.of(path), Glob.of(tmpDir).resolve("a", "b", "c.txt").find());
  }

  @Test
  void testResolve() {
    var base = Glob.of(Path.of("a", "b"));
    var separator = base.base().getFileSystem().getSeparator();
    assertEquals(new Glob(Path.of("a", "b", "c"), null), base.resolve("c"));
    assertEquals(new Glob(Path.of("a", "b", "c", "d"), null), base.resolve("c", "d"));
    assertEquals(new Glob(Path.of("a", "b"), "*" + separator + "d"), base.resolve("*", "d"));
    assertEquals(new Glob(tmpDir, String.join(separator, "*", "*", "c.txt")),
      Glob.of(tmpDir).resolve("*", "*", "c.txt"));
  }

  @Test
  void testParseAbsoluteString() {
    var base = Glob.of(Path.of("a", "b")).resolve("*", "d");
    var separator = base.base().getFileSystem().getSeparator();
    assertEquals(new Glob(base.base().toAbsolutePath(), base.pattern()),
      Glob.parse(base.base().toAbsolutePath() + separator + base.pattern()));
  }
}
