package com.onthegomap.planetiler.util;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.onthegomap.planetiler.archive.TileArchiveConfig;
import com.onthegomap.planetiler.archive.TileArchiveMetadata;
import com.onthegomap.planetiler.archive.TileArchives;
import com.onthegomap.planetiler.archive.TileEncodingResult;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.TileCoord;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.Envelope;

class TileArchiveCopierTest {
  @TempDir
  Path tmpDir;

  @ParameterizedTest
  @CsvSource({
    "out.mbtiles,out2.mbtiles",
    "out.pmtiles,out2.pmtiles",
    "out.mbtiles,out2.pmtiles",
    "out.pmtiles,out2.mbtiles",
  })
  void testCopy(String source, String dest) throws IOException {
    source = tmpDir.resolve(source).toString();
    dest = tmpDir.resolve(dest).toString();

    var config = PlanetilerConfig.defaults();
    var metadata = new TileArchiveMetadata(
      "MyName",
      "MyDescription",
      "MyAttribution",
      "MyVersion",
      "baselayer",
      TileArchiveMetadata.MVT_FORMAT,
      new Envelope(-180, 0, 0, 90),
      new CoordinateXY(-90, 45),
      7d,
      5,
      6,
      List.of(new LayerStats.VectorLayer("MyLayer", Map.of())),
      Map.of("other key", "other value")
    );
    try (
      var writer = TileArchives.newWriter(source, config);
    ) {
      writer.initialize(metadata);
      try (var tileWriter = writer.newTileWriter()) {
        tileWriter.write(new TileEncodingResult(
          TileCoord.ofXYZ(5, 5, 5),
          new byte[]{5, 5, 5},
          OptionalLong.of(1)
        ));
        tileWriter.write(new TileEncodingResult(
          TileCoord.ofXYZ(5, 5, 6),
          new byte[]{5, 5, 6},
          OptionalLong.of(2)
        ));
        tileWriter.write(new TileEncodingResult(
          TileCoord.ofXYZ(6, 5, 6),
          new byte[]{5, 5, 6},
          OptionalLong.of(2)
        ));
      }
      writer.finish(metadata);
    }

    for (boolean deduplicate : new boolean[]{false, true}) {
      String info = "deduplicates=" + deduplicate;
      TileArchiveConfig.from(dest).delete();
      TileArchiveCopier.copy(source, dest, config, deduplicate);
      try (var reader = TileArchives.newReader(dest, config)) {
        assertEquals(metadata, reader.metadata());
        assertArrayEquals(new byte[]{5, 5, 5}, reader.getTile(5, 5, 5), info);
        assertArrayEquals(new byte[]{5, 5, 6}, reader.getTile(5, 5, 6), info);
        assertArrayEquals(new byte[]{5, 5, 6}, reader.getTile(6, 5, 6), info);
        assertEquals(3, reader.getAllTileCoords().stream().count(), info);
      }
    }
  }
}
