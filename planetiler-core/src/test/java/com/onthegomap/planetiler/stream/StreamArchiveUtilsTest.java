package com.onthegomap.planetiler.stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.onthegomap.planetiler.archive.TileArchiveConfig;
import com.onthegomap.planetiler.config.Arguments;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

class StreamArchiveUtilsTest {

  @ParameterizedTest
  @ArgumentsSource(GetEscpacedStringTestArgsProvider.class)
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

  private static class GetEscpacedStringTestArgsProvider implements ArgumentsProvider {

    @Override
    public Stream<? extends org.junit.jupiter.params.provider.Arguments> provideArguments(ExtensionContext context) {
      return Stream.of(
        argsOf("a", "a"),
        argsOf("'a'", "a"),
        argsOf("' '", " "),
        argsOf("'\\n'", "\n")
      );
    }

    private static org.junit.jupiter.params.provider.Arguments argsOf(String in, String out) {
      return org.junit.jupiter.params.provider.Arguments.of(in, out);
    }
  }
}
