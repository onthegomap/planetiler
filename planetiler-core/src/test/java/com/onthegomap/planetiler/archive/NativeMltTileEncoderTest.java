package com.onthegomap.planetiler.archive;

import static com.onthegomap.planetiler.TestUtils.assumeNativeMlt;
import static com.onthegomap.planetiler.TestUtils.decodeMlt;
import static com.onthegomap.planetiler.TestUtils.newLineString;
import static com.onthegomap.planetiler.TestUtils.newPoint;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.onthegomap.planetiler.TestUtils;
import com.onthegomap.planetiler.VectorTile;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.GeometryType;
import com.onthegomap.planetiler.stats.Stats;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class NativeMltTileEncoderTest extends MltTileEncoderTest {

  @BeforeEach
  void requireNativeLibrary() {
    assumeNativeMlt();
  }

  @Override
  TileEncoder encoder(PlanetilerConfig config, Stats stats) {
    return new NativeMltTileEncoder(config, stats);
  }

  @Test
  void widensMixedIntegersAndFloatsOfOneKeyToDoubleAndOtherMixesToText() {
    var tile = tileWith(
      feature("layer", 1, newPoint(1, 2), Map.of("width", 3, "ref", "A1")),
      feature("layer", 2, newPoint(3, 4), Map.of("width", 1.5, "ref", 12)),
      feature("layer", 3, newPoint(5, 6), Map.of("width", 2, "ref", 1.5)));

    var decoded = decodeMlt(encode(tile).bytes());

    assertEquals(List.of(
      TestUtils.feature(newPoint(1, 2), "layer", Map.of("width", 3.0, "ref", "A1"), 1),
      TestUtils.feature(newPoint(3, 4), "layer", Map.of("width", 1.5, "ref", "12"), 2),
      TestUtils.feature(newPoint(5, 6), "layer", Map.of("width", 2.0, "ref", "1.5"), 3)
    ), decoded);
  }

  @Test
  void leavesOutFeaturesWithInvalidGeometryWithoutMovingTheirAttributes() {
    int lineToOnePoint = (1 << 3) | 2; // MVT command header: count 1, command id 2 (LineTo)
    int zigZag2 = 4;
    // a line must start with a MoveTo
    var lineWithoutMoveTo = new VectorTile.VectorGeometry(new int[]{lineToOnePoint, zigZag2, zigZag2}, GeometryType.LINE,
      0);
    var tile = new VectorTile().addLayerFeatures("layer", List.of(
      feature("layer", 1, newLineString(0, 0, 10, 10), Map.of("name", "first")),
      new VectorTile.Feature("layer", 2, lineWithoutMoveTo, Map.of("name", "invalid")),
      feature("layer", 3, newLineString(5, 5, 20, 20), Map.of("name", "third"))));

    var decoded = decodeMlt(encode(tile).bytes());

    // if the invalid feature's name were still written, it would land on feature 1
    assertEquals(List.of(
      TestUtils.feature(newLineString(0, 0, 10, 10), "layer", Map.of("name", "first"), 1),
      TestUtils.feature(newLineString(5, 5, 20, 20), "layer", Map.of("name", "third"), 3)
    ), decoded);
    assertEquals(Map.of("mlt_feature_native_encode", 1L), stats.dataErrors());
  }
}
