package com.onthegomap.planetiler.copy;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.onthegomap.planetiler.TestUtils;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.util.Gzip;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

class TileCopyTest {

  private static final String ARCHIVE_0_JSON_BASE = """
    {"type":"initialization"}
    {"type":"tile","x":0,"y":0,"z":0,"encodedData":"AA=="}
    {"type":"tile","x":1,"y":2,"z":3,"encodedData":"AQ=="}
    {"type":"finish","metadata":%s}
    """;

  private static final String ARCHIVE_0_CSV_COMPRESSION_NONE = """
    0,0,0,AA==
    1,2,3,AQ==
    """;

  private static final String EXTERNAL_METADATA = "{\"name\": \"blub\"}";

  @ParameterizedTest(name = "{index} - {0}")
  @ArgumentsSource(TestArgs.class)
  void testSimple(String testName, String archiveDataIn, String archiveDataOut, Map<String, String> arguments,
    @TempDir Path tempDir) throws Exception {

    final Path archiveInPath = tempDir.resolve(archiveDataIn.contains("{") ? "in.json" : "in.csv");
    final Path archiveOutPath = tempDir.resolve(archiveDataOut.contains("{") ? "out.json" : "out.csv");
    final Path inMetadataPath = tempDir.resolve("metadata.json");

    Files.writeString(archiveInPath, archiveDataIn);
    Files.writeString(inMetadataPath, EXTERNAL_METADATA);

    arguments = new LinkedHashMap<>(arguments);
    arguments.replace("in_metadata", inMetadataPath.toString());

    final Arguments args = Arguments.of(Map.of(
      "input", archiveInPath.toString(),
      "output", archiveOutPath.toString()
    )).orElse(Arguments.of(arguments));

    new TileCopy(TileCopyConfig.fromArguments(args)).run();

    if (archiveDataOut.contains("{")) {
      final List<String> expectedLines = Arrays.stream(archiveDataOut.split("\n")).toList();
      final List<String> actualLines = Files.readAllLines(archiveOutPath);
      assertEquals(expectedLines.size(), actualLines.size());
      for (int i = 0; i < expectedLines.size(); i++) {
        TestUtils.assertSameJson(expectedLines.get(i), actualLines.get(i));
      }
    } else {
      assertEquals(archiveDataOut, Files.readString(archiveOutPath));
    }
  }

  private static String compressBase64(String archiveIn) {
    final Base64.Encoder encoder = Base64.getEncoder();
    for (int i = 0; i <= 1; i++) {
      archiveIn = archiveIn.replace(
        encoder.encodeToString(new byte[]{(byte) i}),
        encoder.encodeToString(Gzip.gzip(new byte[]{(byte) i}))
      );
    }
    return archiveIn;
  }

  private static String replaceBase64(String archiveIn, String replacement) {
    final Base64.Encoder encoder = Base64.getEncoder();
    for (int i = 0; i <= 1; i++) {
      archiveIn = archiveIn.replace(
        encoder.encodeToString(new byte[]{(byte) i}),
        replacement
      );
    }
    return archiveIn;
  }

  private static class TestArgs implements ArgumentsProvider {

    @Override
    public Stream<? extends org.junit.jupiter.params.provider.Arguments> provideArguments(ExtensionContext context) {
      return Stream.of(
        argsOf(
          "json(w/o meta, compression:none) to csv(compression:none)",
          ARCHIVE_0_JSON_BASE.formatted("null"),
          ARCHIVE_0_CSV_COMPRESSION_NONE,
          Map.of("out_tile_compression", "none")
        ),
        argsOf(
          "json(w/o meta, compression:none) to csv(compression:gzip)",
          ARCHIVE_0_JSON_BASE.formatted("null"),
          compressBase64(ARCHIVE_0_CSV_COMPRESSION_NONE),
          Map.of("out_tile_compression", "gzip")
        ),
        argsOf(
          "json(w/o meta, compression:gzip) to csv(compression:none)",
          compressBase64(ARCHIVE_0_JSON_BASE.formatted("null")),
          ARCHIVE_0_CSV_COMPRESSION_NONE,
          Map.of("out_tile_compression", "none")
        ),
        argsOf(
          "json(w/o meta, compression:gzip) to csv(compression:gzip)",
          compressBase64(ARCHIVE_0_JSON_BASE.formatted("null")),
          compressBase64(ARCHIVE_0_CSV_COMPRESSION_NONE),
          Map.of("out_tile_compression", "gzip")
        ),
        argsOf(
          "json(w/o meta, compression:gzip) to csv(compression:gzip)",
          compressBase64(ARCHIVE_0_JSON_BASE.formatted("null")),
          compressBase64(ARCHIVE_0_CSV_COMPRESSION_NONE),
          Map.of("out_tile_compression", "gzip")
        ),
        argsOf(
          "json(w/ meta, compression:gzip) to csv(compression:none)",
          compressBase64(ARCHIVE_0_JSON_BASE.formatted(TestUtils.MAX_METADATA_SERIALIZED)),
          ARCHIVE_0_CSV_COMPRESSION_NONE,
          Map.of("out_tile_compression", "none")
        ),
        argsOf(
          "json(w/ meta, compression:gzip) to json(w/ meta, compression:gzip)",
          compressBase64(ARCHIVE_0_JSON_BASE.formatted(TestUtils.MAX_METADATA_SERIALIZED)),
          compressBase64(ARCHIVE_0_JSON_BASE.formatted(TestUtils.MAX_METADATA_SERIALIZED)),
          Map.of()
        ),
        argsOf(
          "csv to json - use fallback metadata",
          ARCHIVE_0_CSV_COMPRESSION_NONE,
          ARCHIVE_0_JSON_BASE.formatted(
            "{\"name\":\"unknown\",\"format\":\"pbf\",\"minzoom\":\"0\",\"maxzoom\":\"14\",\"json\":\"{\\\"vector_layers\\\":[]}\",\"compression\":\"none\"}"),
          Map.of("out_tile_compression", "none")
        ),
        argsOf(
          "csv to json - use external metadata",
          ARCHIVE_0_CSV_COMPRESSION_NONE,
          ARCHIVE_0_JSON_BASE
            .formatted("{\"name\":\"blub\",\"compression\":\"none\",\"minzoom\":\"0\",\"maxzoom\":\"14\"}"),
          Map.of("out_tile_compression", "none", "in_metadata", "blub")
        ),
        argsOf(
          "csv to json - null handling",
          replaceBase64(ARCHIVE_0_CSV_COMPRESSION_NONE, ""),
          replaceBase64(ARCHIVE_0_JSON_BASE
            .formatted("{\"name\":\"blub\",\"compression\":\"gzip\",\"minzoom\":\"0\",\"maxzoom\":\"14\"}"), "null")
            .replace(",\"encodedData\":\"null\"", ""),
          Map.of("in_metadata", "blub", "skip_empty", "false")
        ),
        argsOf(
          "json to csv - null handling",
          replaceBase64(ARCHIVE_0_JSON_BASE.formatted("null"), "null")
            .replace(",\"encodedData\":\"null\"", ""),
          replaceBase64(ARCHIVE_0_CSV_COMPRESSION_NONE, ""),
          Map.of("skip_empty", "false")
        ),
        argsOf(
          "csv to csv - empty skipping on",
          """
            0,0,0,
            1,2,3,AQ==
            """,
          """
            1,2,3,AQ==
            """,
          Map.of("skip_empty", "true", "out_tile_compression", "none")
        ),
        argsOf(
          "csv to csv - empty skipping off",
          """
            0,0,0,
            1,2,3,AQ==
            """,
          """
            0,0,0,
            1,2,3,AQ==
            """,
          Map.of("skip_empty", "false", "out_tile_compression", "none")
        ),
        argsOf(
          "tiles in order",
          """
            1,2,3,AQ==
            0,0,0,AA==
            """,
          """
            1,2,3,AQ==
            0,0,0,AA==
            """,
          Map.of("out_tile_compression", "none")
        ),
        argsOf(
          "tiles re-order",
          """
            0,0,1,AQ==
            0,0,0,AA==
            """,
          """
            0,0,0,AA==
            0,0,1,AQ==
            """,
          Map.of("out_tile_compression", "none", "scan_tiles_in_order", "false", "filter_maxzoom", "1")
        ),
        argsOf(
          "filter min zoom",
          """
            0,0,0,AA==
            0,0,1,AQ==
            """,
          """
            0,0,1,AQ==
            """,
          Map.of("out_tile_compression", "none", "filter_minzoom", "1")
        ),
        argsOf(
          "filter max zoom",
          """
            0,0,1,AQ==
            0,0,0,AA==
            """,
          """
            0,0,0,AA==
            """,
          Map.of("out_tile_compression", "none", "filter_maxzoom", "0")
        )
      );
    }

    private static org.junit.jupiter.params.provider.Arguments argsOf(String testName, String archiveDataIn,
      String archiveDataOut, Map<String, String> arguments) {
      return org.junit.jupiter.params.provider.Arguments.of(testName, archiveDataIn, archiveDataOut, arguments);
    }
  }

}
