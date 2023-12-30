package com.onthegomap.planetiler.files;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.geo.TileOrder;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class TileSchemeEncodingTest {

  @ParameterizedTest
  @CsvSource(textBlock = """
    {z}/{x}/{y}.pbf,    3/1/2.pbf
    {x}/{y}/{z}.pbf,    1/2/3.pbf
    {x}-{y}-{z}.pbf,    1-2-3.pbf
    {x}/a/{y}/b{z}.pbf, 1/a/2/b3.pbf
    {z}/{x}/{y}.pbf.gz, 3/1/2.pbf.gz
    {z}/{xs}/{ys}.pbf,  3/000/001/000/002.pbf
    {z}/{x}/{ys}.pbf,   3/1/000/002.pbf
    {z}/{xs}/{y}.pbf,   3/000/001/2.pbf
    """
  )
  void testEncoder(String tileScheme, Path tilePath, @TempDir Path tempDir) {
    final Path tilesDir = tempDir.resolve("tiles");
    tilePath = tilesDir.resolve(tilePath);
    assertEquals(
      tilePath,
      new TileSchemeEncoding(tileScheme, tilesDir).encoder().apply(TileCoord.ofXYZ(1, 2, 3))
    );
  }

  @ParameterizedTest
  @CsvSource(textBlock = """
    {z}/{x}/{y}.pbf,    3/1/2.pbf,              true
    {x}/{y}/{z}.pbf,    1/2/3.pbf,              true
    {x}-{y}-{z}.pbf,    1-2-3.pbf,              true
    {x}/a/{y}/b{z}.pbf, 1/a/2/b3.pbf,           true
    {z}/{x}/{y}.pbf.gz, 3/1/2.pbf.gz,           true
    {z}/{xs}/{ys}.pbf,  3/000/001/000/002.pbf,  true
    {z}/{x}/{ys}.pbf,   3/1/000/002.pbf,        true
    {z}/{xs}/{y}.pbf,   3/000/001/2.pbf,        true

    {z}/{x}/{y}.pbf,    3/1/2.pb,               false
    {z}/{x}/{y}.pbf,    3/1/2,                  false
    {z}/{x}/{y}.pbf,    a/1/2.pbf,              false
    {z}/{x}/{y}.pbf,    3/a/2.pbf,              false
    {z}/{x}/{y}.pbf,    3/1/a.pbf,              false
    """
  )
  void testDecoder(String tileScheme, Path tilePath, boolean valid, @TempDir Path tempDir) {
    final Path tilesDir = tempDir.resolve("tiles");
    tilePath = tilesDir.resolve(tilePath);
    if (valid) {
      assertEquals(
        Optional.of(TileCoord.ofXYZ(1, 2, 3)),
        new TileSchemeEncoding(tileScheme, tilesDir).decoder().apply(tilePath)
      );
    } else {
      assertEquals(
        Optional.empty(),
        new TileSchemeEncoding(tileScheme, tilesDir).decoder().apply(tilePath)
      );
    }
  }

  @ParameterizedTest
  @CsvSource(textBlock = """
    {z}/{x}/{y}.pbf,    3
    {x}/{y}/{z}.pbf,    3
    {x}-{y}-{z}.pbf,    1
    {x}/a/{y}/b{z}.pbf, 4
    {z}/{x}/{y}.pbf.gz, 3
    {z}/{xs}/{ys}.pbf,  5
    {z}/{x}/{ys}.pbf,   4
    {z}/{xs}/{y}.pbf,   4
    """
  )
  void testSearchDepth(String tileScheme, int searchDepth, @TempDir Path tempDir) {
    final Path tilesDir = tempDir.resolve("tiles");
    assertEquals(
      searchDepth,
      new TileSchemeEncoding(tileScheme, tilesDir).searchDepth()
    );
  }

  @ParameterizedTest
  @CsvSource(textBlock = """
    1/2/3.pbf
    {x}/{y}.pbf
    {z}/{y}.pbf
    {z}/{x}
    {z}/{x}/1.pbf
    {z}/{x}/{y}/{xs}.pbf
    {z}/{x}/{y}/{ys}.pbf
    {z}/{z}/{x}/{y}.pbf
    {x}/{z}/{x}/{y}.pbf
    {y}/{z}/{x}/{y}.pbf
    {xs}/{z}/{xs}/{ys}.pbf
    {ys}/{z}/{xs}/{ys}.pbf
    {x}/\\Q/{y}/b{z}.pbf
    {x}/\\E/{y}/b{z}.pbf
    """
  )
  void testInvalidSchemes(String tileScheme, @TempDir Path tempDir) {
    final Path tilesDir = tempDir.resolve("tiles");
    assertThrows(Exception.class, () -> new TileSchemeEncoding(tileScheme, tilesDir));
  }

  @Test
  void testInvalidAbsoluteTileScheme(@TempDir Path tempDir) {
    final Path tilesDir = tempDir.resolve("tiles");
    final String tileSchemeAbsolute = tilesDir.resolve(Paths.get("{z}", "{x}", "{y}.pbf")).toAbsolutePath().toString();
    assertThrows(Exception.class, () -> new TileSchemeEncoding(tileSchemeAbsolute, tilesDir));
  }

  @ParameterizedTest
  @CsvSource({
    "{z}/{x}/{y}.pbf,   TMS",
    "{z}/{xs}/{ys}.pbf, TMS",
    // given there is no (suitable) other tile order yet - use TMS here as wel
    "{x}/{y}/{z}.pbf,   TMS"
  })
  void testPreferredTileOrder(String tileScheme, TileOrder tileOrder, @TempDir Path tempDir) {
    final Path tilesDir = tempDir.resolve("tiles");
    assertEquals(
      tileOrder,
      new TileSchemeEncoding(tileScheme, tilesDir).preferredTileOrder()

    );
  }
}
