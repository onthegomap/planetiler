package com.onthegomap.planetiler.files;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.onthegomap.planetiler.config.Arguments;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class FilesArchiveUtilsTest {

  @ParameterizedTest
  @CsvSource(textBlock = """
    {z}/{x}/{y}.pbf           ,      , {z}/{x}/{y}.pbf
                              ,      , {z}/{x}/{y}.pbf
    {x}/{y}/{z}.pbf           ,      , {x}/{y}/{z}.pbf
    tiles/{z}/{x}/{y}.pbf     , tiles, {z}/{x}/{y}.pbf
    tiles/z{z}/{x}/{y}.pbf    , tiles, z{z}/{x}/{y}.pbf
    z{z}/x{x}/y{y}.pbf        ,      , z{z}/x{x}/y{y}.pbf
    tiles/tile-{z}-{x}-{y}.pbf, tiles, tile-{z}-{x}-{y}.pbf
    /a                        , /a   , {z}/{x}/{y}.pbf
    /                         , /    , {z}/{x}/{y}.pbf
    """
  )
  void testBasePathWithTileSchemeEncoding(String shortcutBase, String actualBase, String tileScheme,
    @TempDir Path tempDir) {

    final Path shortcutBasePath = makePath(shortcutBase, tempDir);
    final Path actualBasePath = makePath(actualBase, tempDir);

    assertEquals(
      new FilesArchiveUtils.BasePathWithTileSchemeEncoding(
        actualBasePath,
        new TileSchemeEncoding(
          Paths.get(tileScheme).toString(),
          actualBasePath
        )
      ),
      FilesArchiveUtils.basePathWithTileSchemeEncoding(Arguments.of(), shortcutBasePath)
    );
  }

  @Test
  void testBasePathWithTileSchemeEncodingPrefersArgOverShortcut() {
    final Path basePath = Paths.get("");
    final Path schemeShortcutPath = Paths.get("{x}", "{y}", "{z}.pbf");
    final Path schemeArgumentPath = Paths.get("x{x}", "y{y}", "z{z}.pbf");
    final Path shortcutPath = basePath.resolve(schemeShortcutPath);
    assertEquals(
      new FilesArchiveUtils.BasePathWithTileSchemeEncoding(
        basePath,
        new TileSchemeEncoding(
          schemeShortcutPath.toString(),
          basePath
        )
      ),
      FilesArchiveUtils.basePathWithTileSchemeEncoding(Arguments.of(), shortcutPath)
    );
    assertEquals(
      new FilesArchiveUtils.BasePathWithTileSchemeEncoding(
        basePath,
        new TileSchemeEncoding(
          schemeArgumentPath.toString(),
          basePath
        )
      ),
      FilesArchiveUtils.basePathWithTileSchemeEncoding(
        Arguments.of(Map.of(FilesArchiveUtils.OPTION_TILE_SCHEME, schemeArgumentPath.toString())), shortcutPath)
    );
  }

  @ParameterizedTest
  @CsvSource(textBlock = """
    {z}/{x}/{y}.pbf           ,
                              ,
    {x}/{y}/{z}.pbf           ,
    tiles/{z}/{x}/{y}.pbf     , tiles
    tiles/z{z}/{x}/{y}.pbf    , tiles
    z{z}/x{x}/y{y}.pbf        ,
    tiles/tile-{z}-{x}-{y}.pbf, tiles
    /a                        , /a
    /                         , /
    """
  )
  void testCleanBasePath(String shortcutBase, String actualBase, @TempDir Path tempDir) {

    assertEquals(
      makePath(actualBase, tempDir),
      FilesArchiveUtils.cleanBasePath(makePath(shortcutBase, tempDir))
    );
  }


  private static Path makePath(String in, @TempDir Path tempDir) {
    if (in == null) {
      return Paths.get("");
    }
    if (in.startsWith("/")) {
      return tempDir.resolve(in.substring(1));
    }
    return Paths.get(in);
  }
}
