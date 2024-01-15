package com.onthegomap.planetiler.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.archive.TileArchiveConfig;
import com.onthegomap.planetiler.archive.TileArchiveMetadata;
import com.onthegomap.planetiler.archive.TileEncodingResult;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.TileOrder;
import com.onthegomap.planetiler.pmtiles.WriteablePmtiles;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import vector_tile.VectorTileProto;

class CompareArchivesTest {
  @TempDir
  Path path;
  PlanetilerConfig config = PlanetilerConfig.defaults();
  byte[] tile1 = VectorTileProto.Tile.newBuilder().addLayers(
    VectorTileProto.Tile.Layer.newBuilder()
      .setVersion(2)
      .setName("layer1")
      .addKeys("key1")
      .addValues(VectorTileProto.Tile.Value.newBuilder().setStringValue("value1"))
      .addFeatures(VectorTileProto.Tile.Feature.newBuilder().setId(1)))
    .build()
    .toByteArray();

  byte[] tile2 = VectorTileProto.Tile.newBuilder().addLayers(
    VectorTileProto.Tile.Layer.newBuilder()
      .setVersion(2)
      .setName("layer1")
      .addKeys("key1")
      .addValues(VectorTileProto.Tile.Value.newBuilder().setStringValue("value2"))
      .addFeatures(VectorTileProto.Tile.Feature.newBuilder().setId(2)))
    .build()
    .toByteArray();

  @Test
  void testCompareArchives() throws IOException {
    var aPath = path.resolve("a.pmtiles");
    var bPath = path.resolve("b.pmtiles");
    try (
      var a = WriteablePmtiles.newWriteToFile(aPath);
      var b = WriteablePmtiles.newWriteToFile(bPath);
    ) {
      a.initialize();
      b.initialize();
      try (
        var aWriter = a.newTileWriter();
        var bWriter = b.newTileWriter()
      ) {
        aWriter
          .write(new TileEncodingResult(TileOrder.HILBERT.decode(0), new byte[]{0xa, 0x2}, OptionalLong.empty()));
        aWriter
          .write(new TileEncodingResult(TileOrder.HILBERT.decode(2), Gzip.gzip(tile1), OptionalLong.empty()));
        aWriter
          .write(new TileEncodingResult(TileOrder.HILBERT.decode(4), new byte[]{0xa, 0x2}, OptionalLong.empty()));
        bWriter.write(new TileEncodingResult(TileOrder.HILBERT.decode(1), new byte[]{0xa, 0x2}, OptionalLong.empty()));
        bWriter.write(new TileEncodingResult(TileOrder.HILBERT.decode(2), Gzip.gzip(tile2), OptionalLong.empty()));
        bWriter.write(new TileEncodingResult(TileOrder.HILBERT.decode(3), new byte[]{0xa, 0x2}, OptionalLong.empty()));
        bWriter
          .write(new TileEncodingResult(TileOrder.HILBERT.decode(4), new byte[]{0xa, 0x2}, OptionalLong.empty()));
      }
      a.finish(new TileArchiveMetadata(new Profile.NullProfile(), config));
      b.finish(new TileArchiveMetadata(new Profile.NullProfile(), config));
    }
    var result = CompareArchives.compare(
      TileArchiveConfig.from(aPath.toString()),
      TileArchiveConfig.from(bPath.toString()),
      config,
      false
    );
    assertEquals(new CompareArchives.Result(
      5, 4, List.of(
        "pmtiles header"
      ), Map.of(
        "archive 2 missing tile", 1L,
        "archive 1 missing tile", 2L,
        "different contents", 1L
      ), Map.of(
        "layer1", Map.of(
          "values list unique values", 1L,
          "feature ids", 1L
        )
      )
    ), result);
  }

  @Test
  void testCompareArchivesDifferentCompression() throws IOException {
    var aPath = path.resolve("a.pmtiles");
    var bPath = path.resolve("b.pmtiles");
    byte[] a1 = new byte[]{0xa, 0x2};
    byte[] b1 = Gzip.gzip(a1);
    byte[] a2 = tile1;
    byte[] b2 = Gzip.gzip(tile2);
    try (
      var a = WriteablePmtiles.newWriteToFile(aPath);
      var b = WriteablePmtiles.newWriteToFile(bPath);
    ) {
      a.initialize();
      b.initialize();
      try (
        var aWriter = a.newTileWriter();
        var bWriter = b.newTileWriter()
      ) {
        aWriter
          .write(new TileEncodingResult(TileOrder.HILBERT.decode(0), a1, OptionalLong.empty()));
        aWriter
          .write(new TileEncodingResult(TileOrder.HILBERT.decode(2), a2, OptionalLong.empty()));
        aWriter
          .write(new TileEncodingResult(TileOrder.HILBERT.decode(4), a1, OptionalLong.empty()));
        bWriter.write(new TileEncodingResult(TileOrder.HILBERT.decode(1), b1, OptionalLong.empty()));
        bWriter.write(new TileEncodingResult(TileOrder.HILBERT.decode(2), b2, OptionalLong.empty()));
        bWriter.write(new TileEncodingResult(TileOrder.HILBERT.decode(3), b1, OptionalLong.empty()));
        bWriter
          .write(new TileEncodingResult(TileOrder.HILBERT.decode(4), b1, OptionalLong.empty()));
      }
      a.finish(new TileArchiveMetadata(new Profile.NullProfile(),
        PlanetilerConfig.from(Arguments.fromArgs("--tile-compression=none"))));
      b.finish(new TileArchiveMetadata(new Profile.NullProfile(), config));
    }
    var result = CompareArchives.compare(
      TileArchiveConfig.from(aPath.toString()),
      TileArchiveConfig.from(bPath.toString()),
      config,
      false
    );
    assertEquals(new CompareArchives.Result(
      5, 4, List.of(
        "metadata",
        "pmtiles header",
        "tile compression"
      ), Map.of(
        "archive 2 missing tile", 1L,
        "archive 1 missing tile", 2L,
        "different decompressed contents", 1L
      ), Map.of(
        "layer1", Map.of(
          "values list unique values", 1L,
          "feature ids", 1L
        )
      )
    ), result);
  }

  @Test
  void testCompareArchivesSame() throws IOException {
    var aPath = path.resolve("a.pmtiles");
    var bPath = path.resolve("b.pmtiles");
    byte[] a1 = new byte[]{0xa, 0x2};
    try (
      var a = WriteablePmtiles.newWriteToFile(aPath);
      var b = WriteablePmtiles.newWriteToFile(bPath);
    ) {
      a.initialize();
      b.initialize();
      try (
        var aWriter = a.newTileWriter();
        var bWriter = b.newTileWriter()
      ) {
        aWriter
          .write(new TileEncodingResult(TileOrder.HILBERT.decode(0), a1, OptionalLong.empty()));
        aWriter
          .write(new TileEncodingResult(TileOrder.HILBERT.decode(2), a1, OptionalLong.empty()));
        bWriter
          .write(new TileEncodingResult(TileOrder.HILBERT.decode(0), a1, OptionalLong.empty()));
        bWriter
          .write(new TileEncodingResult(TileOrder.HILBERT.decode(2), a1, OptionalLong.empty()));
      }
      a.finish(new TileArchiveMetadata(new Profile.NullProfile(), config));
      b.finish(new TileArchiveMetadata(new Profile.NullProfile(), config));
    }
    var result = CompareArchives.compare(
      TileArchiveConfig.from(aPath.toString()),
      TileArchiveConfig.from(bPath.toString()),
      config,
      false
    );
    assertEquals(new CompareArchives.Result(
      2, 0, List.of(), Map.of(), Map.of()
    ), result);
  }
}
