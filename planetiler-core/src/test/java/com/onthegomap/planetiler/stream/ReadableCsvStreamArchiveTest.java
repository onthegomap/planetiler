package com.onthegomap.planetiler.stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.onthegomap.planetiler.archive.Tile;
import com.onthegomap.planetiler.archive.TileArchiveConfig;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.geo.TileCoord;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.commons.text.StringEscapeUtils;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class ReadableCsvStreamArchiveTest {


  @ParameterizedTest
  @CsvSource(delimiter = '$', textBlock = """
    ,$\\n$false$BASE64
    ,$\\r$false$BASE64
    ,$\\r\\n$false$BASE64
    ,$x$false$BASE64
    ;$\\n$false$BASE64
    {$\\n$false$BASE64
    ,${$false$BASE64
    ,$\\n$false$HEX
    ,$\\n$true$BASE64
    """)
  void testSimple(String columnSeparator, String lineSeparator, boolean pad, CsvBinaryEncoding encoding,
    @TempDir Path tempDir) throws IOException {

    final Path csvFile = tempDir.resolve("in.csv");
    final String csv =
      """
        0,0,0,AA==
        1,2,3,AQ==
        """
        .replace("\n", StringEscapeUtils.unescapeJava(lineSeparator))
        .replace(",", columnSeparator + (pad ? " " : ""))
        .replace("AA==", encoding == CsvBinaryEncoding.BASE64 ? "AA==" : "00")
        .replace("AQ==", encoding == CsvBinaryEncoding.BASE64 ? "AQ==" : "01");

    Files.writeString(csvFile, csv);
    final StreamArchiveConfig config = new StreamArchiveConfig(
      false,
      Arguments.of(Map.of(
        StreamArchiveUtils.CSV_OPTION_COLUMN_SEPARATOR, columnSeparator,
        StreamArchiveUtils.CSV_OPTION_LINE_SEPARATOR, lineSeparator,
        StreamArchiveUtils.CSV_OPTION_BINARY_ENCODING, encoding.id()
      ))
    );

    final List<Tile> expectedTiles = List.of(
      new Tile(TileCoord.ofXYZ(0, 0, 0), new byte[]{0}),
      new Tile(TileCoord.ofXYZ(1, 2, 3), new byte[]{1})
    );

    try (var reader = ReadableCsvArchive.newReader(TileArchiveConfig.Format.CSV, csvFile, config)) {
      try (var s = reader.getAllTiles().stream()) {
        assertEquals(expectedTiles, s.toList());
      }
      try (var s = reader.getAllTiles().stream()) {
        assertEquals(expectedTiles, s.toList());
      }
      assertNull(reader.metadata());
      assertNull(reader.metadata());
      assertArrayEquals(expectedTiles.get(1).bytes(), reader.getTile(TileCoord.ofXYZ(1, 2, 3)));
      assertArrayEquals(expectedTiles.get(0).bytes(), reader.getTile(0, 0, 0));
      assertNull(reader.getTile(4, 5, 6));
    }
  }
}
