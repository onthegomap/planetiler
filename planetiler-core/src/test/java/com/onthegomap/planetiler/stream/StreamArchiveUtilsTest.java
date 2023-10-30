package com.onthegomap.planetiler.stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.onthegomap.planetiler.archive.TileArchiveConfig;
import com.onthegomap.planetiler.config.Arguments;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class StreamArchiveUtilsTest {

  @ParameterizedTest
  @CsvSource(value = {"a,a", "'a',a", "' ',$ $", "'\\n',$\n$"}, quoteCharacter = '$')
  void testGetEscpacedString(String in, String out) {

    final Arguments options = Arguments.of(Map.of("key", in));

    assertEquals(out, StreamArchiveUtils.getEscapedString(options, TileArchiveConfig.Format.CSV, "key", "descr.", "ex",
      List.of("\n", " ")));
  }

  @Test
  void testConstructIndexedPath(@TempDir Path tempDir) {
    final Path base = tempDir.resolve("base.test");
    assertEquals(base, StreamArchiveUtils.constructIndexedPath(base, 0));
    assertEquals(tempDir.resolve("base.test" + 1), StreamArchiveUtils.constructIndexedPath(base, 1));
    assertEquals(tempDir.resolve("base.test" + 13), StreamArchiveUtils.constructIndexedPath(base, 13));
  }
}
