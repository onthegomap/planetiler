package com.onthegomap.planetiler.render;

import static com.onthegomap.planetiler.TestUtils.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.TestUtils;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.geo.GeometryPipeline;
import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.reader.SimpleFeature;
import com.onthegomap.planetiler.stats.Stats;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.geom.util.AffineTransformation;
import org.locationtech.jts.precision.GeometryPrecisionReducer;

class FeatureRendererTest {

  private PlanetilerConfig config = PlanetilerConfig.defaults();
  private final Stats stats = Stats.inMemory();

  private FeatureCollector collector(Geometry worldGeom) {
    var latLonGeom = GeoUtils.worldToLatLonCoords(worldGeom);
    return new FeatureCollector.Factory(config, stats)
      .get(SimpleFeature.create(latLonGeom, HashMap.newHashMap(0), null, null,
        1));
  }

  private Map<TileCoord, Collection<Geometry>> renderGeometry(FeatureCollector.Feature feature) {
    Map<TileCoord, Collection<Geometry>> result = new TreeMap<>();
    new FeatureRenderer(config, rendered -> result.computeIfAbsent(rendered.tile(), tile -> new HashSet<>())
      .add(decodeSilently(rendered.vectorTileFeature().geometry())), Stats.inMemory()).accept(feature);
    result.values().forEach(gs -> gs.forEach(TestUtils::validateGeometry));
    return result;
  }

  private Map<TileCoord, Collection<RenderedFeature>> renderFeatures(FeatureCollector.Feature feature) {
    Map<TileCoord, Collection<RenderedFeature>> result = new TreeMap<>();
    new FeatureRenderer(config, rendered -> result.computeIfAbsent(rendered.tile(), tile -> new HashSet<>())
      .add(rendered), Stats.inMemory()).accept(feature);
    result.values()
      .forEach(gs -> gs.forEach(f -> TestUtils.validateGeometry(decodeSilently(f.vectorTileFeature().geometry()))));
    return result;
  }

  private Set<List<?>> renderedTileFeatures(FeatureCollector.Feature feature, TileCoord coord) {
    return renderFeatures(feature).get(coord).stream()
      .map(RenderedFeature::vectorTileFeature)
      .map(d -> {
        try {
          return List.of(d.geometry().decode(), d.tags());
        } catch (GeometryException e) {
          throw new RuntimeException(e);
        }
      })
      .collect(Collectors.toSet());
  }

  private static final int Z14_TILES = 1 << 14;
  private static final double Z14_WIDTH = 1d / Z14_TILES;
  private static final double Z14_PX = Z14_WIDTH / 256;
  private static final int Z13_TILES = 1 << 13;
  private static final double Z13_WIDTH = 1d / Z13_TILES;
  private static final double Z13_PX = Z13_WIDTH / 256;

  @Test
  void testEmptyGeometry() {
    var feature = collector(GeoUtils.JTS_FACTORY.createPoint()).point("layer");
    assertSameNormalizedFeatures(Map.of(), renderGeometry(feature));
  }

  /*
   * POINT TESTS
   */

  @Test
  void testSinglePoint() {
    var feature = pointFeature(newPoint(0.5 + Z14_WIDTH / 2, 0.5 + Z14_WIDTH / 2))
      .setZoomRange(14, 14);
    assertSameNormalizedFeatures(Map.of(
      TileCoord.ofXYZ(Z14_TILES / 2, Z14_TILES / 2, 14), List.of(
        newPoint(128, 128)
      )
    ), renderGeometry(feature));
  }

  @Test
  void testRepeatSinglePointNeighboringTiles() {
    var feature = pointFeature(newPoint(0.5 + 1d / 512, 0.5 + 1d / 512))
      .setZoomRange(0, 1)
      .setBufferPixels(2);
    assertSameNormalizedFeatures(Map.of(
      TileCoord.ofXYZ(0, 0, 0), List.of(newPoint(128.5, 128.5)),
      TileCoord.ofXYZ(0, 0, 1), List.of(newPoint(257, 257)),
      TileCoord.ofXYZ(1, 0, 1), List.of(newPoint(1, 257)),
      TileCoord.ofXYZ(0, 1, 1), List.of(newPoint(257, 1)),
      TileCoord.ofXYZ(1, 1, 1), List.of(newPoint(1, 1))
    ), renderGeometry(feature));
  }

  @Test
  void testRepeatSinglePointNeighboringTilesBuffer0() {
    var feature = pointFeature(newPoint(0.5, 0.5))
      .setZoomRange(1, 1)
      .setBufferPixels(0);
    assertSameNormalizedFeatures(Map.of(
      TileCoord.ofXYZ(0, 0, 1), List.of(newPoint(256, 256)),
      TileCoord.ofXYZ(1, 0, 1), List.of(newPoint(0, 256)),
      TileCoord.ofXYZ(0, 1, 1), List.of(newPoint(256, 0)),
      TileCoord.ofXYZ(1, 1, 1), List.of(newPoint(0, 0))
    ), renderGeometry(feature));
  }

  @Test
  void testEmitPointsRespectExtents() {
    config = PlanetilerConfig.from(Arguments.of(
      "bounds", "0,-80,180,0"
    ));
    var feature = pointFeature(newPoint(0.5 + 1d / 512, 0.5 + 1d / 512))
      .setZoomRange(0, 1)
      .setBufferPixels(2);
    assertSameNormalizedFeatures(Map.of(
      TileCoord.ofXYZ(0, 0, 0), List.of(newPoint(128.5, 128.5)),
      TileCoord.ofXYZ(1, 1, 1), List.of(newPoint(1, 1))
    ), renderGeometry(feature));
  }

  @Test
  void testEmitPointsRespectShape() {
    config = PlanetilerConfig.from(Arguments.of(
      "polygon", TestUtils.pathToResource("bottomrightearth.poly")
    ));
    var feature = pointFeature(newPoint(0.5 + 1d / 512, 0.5 + 1d / 512))
      .setZoomRange(0, 2)
      .setBufferPixels(2);
    assertSameNormalizedFeatures(Map.of(
      TileCoord.ofXYZ(0, 0, 0), List.of(newPoint(128.5, 128.5)),
      TileCoord.ofXYZ(1, 1, 1), List.of(newPoint(1, 1))
    ), renderGeometry(feature));
  }

  @TestFactory
  List<DynamicTest> testProcessPointsNearInternationalDateLineAndPoles() {
    double d = 1d / 512;
    record X(double x, double wrapped, double z1x0, double z1x1) {}
    record Y(double y, int z1ty, double tyoff) {}
    var xs = List.of(
      new X(-d, 1 - d, -1, 255),
      new X(d, 1 + d, 1, 257),
      new X(1 - d, -d, -1, 255),
      new X(1 + d, d, 1, 257)
    );
    var ys = List.of(
      new Y(0.25, 0, 128),
      new Y(-d, 0, -1),
      new Y(d, 0, 1),
      new Y(1 - d, 1, 255),
      new Y(1 + d, 1, 257)
    );
    List<DynamicTest> tests = new ArrayList<>();
    for (X x : xs) {
      for (Y y : ys) {
        tests.add(dynamicTest((x.x * 256) + ", " + (y.y * 256), () -> {
          var feature = pointFeature(newPoint(x.x, y.y))
            .setZoomRange(0, 1)
            .setBufferPixels(2);
          assertSameNormalizedFeatures(Map.of(
            TileCoord.ofXYZ(0, 0, 0), List.of(newMultiPoint(
              newPoint(x.x * 256, y.y * 256),
              newPoint(x.wrapped * 256, y.y * 256)
            )),
            TileCoord.ofXYZ(0, y.z1ty, 1), List.of(newPoint(x.z1x0, y.tyoff)),
            TileCoord.ofXYZ(1, y.z1ty, 1), List.of(newPoint(x.z1x1, y.tyoff))
          ), renderGeometry(feature));
        }));
      }
    }

    return tests;
  }

  @Test
  void testZ0FullTileBuffer() {
    var feature = pointFeature(newPoint(0.25, 0.25))
      .setZoomRange(0, 1)
      .setBufferPixels(256);
    assertSameNormalizedFeatures(Map.of(
      TileCoord.ofXYZ(0, 0, 0), List.of(newMultiPoint(
        newPoint(-192, 64),
        newPoint(64, 64),
        newPoint(320, 64)
      )),
      TileCoord.ofXYZ(0, 0, 1), List.of(newPoint(128, 128)),
      TileCoord.ofXYZ(1, 0, 1), List.of(newMultiPoint(
        newPoint(-128, 128),
        newPoint(256 + 128, 128)
      )),
      TileCoord.ofXYZ(0, 1, 1), List.of(newPoint(128, -128)),
      TileCoord.ofXYZ(1, 1, 1), List.of(newMultiPoint(
        newPoint(-128, -128),
        newPoint(256 + 128, -128)
      ))
    ), renderGeometry(feature));
  }

  @Test
  void testMultipointNoLabelGrid() {
    var feature = pointFeature(newMultiPoint(
      newPoint(0.25, 0.25),
      newPoint(0.25 + 1d / 256, 0.25 + 1d / 256)
    ))
      .setZoomRange(0, 1)
      .setBufferPixels(4);
    assertSameNormalizedFeatures(Map.of(
      TileCoord.ofXYZ(0, 0, 0), List.of(newMultiPoint(
        newPoint(64, 64),
        newPoint(65, 65)
      )),
      TileCoord.ofXYZ(0, 0, 1), List.of(newMultiPoint(
        newPoint(128, 128),
        newPoint(130, 130)
      ))
    ), renderGeometry(feature));
  }

  @Test
  void testMultipointWithLabelGridSplits() {
    var feature = pointFeature(newMultiPoint(
      newPoint(0.25, 0.25),
      newPoint(0.25 + 1d / 256, 0.25 + 1d / 256)
    ))
      .setPointLabelGridPixelSize(10, 10)
      .setZoomRange(0, 1)
      .setBufferPixels(10);
    assertSameNormalizedFeatures(Map.of(
      TileCoord.ofXYZ(0, 0, 0), List.of(
        newPoint(64, 64),
        newPoint(65, 65)
      ),
      TileCoord.ofXYZ(0, 0, 1), List.of(
        newPoint(128, 128),
        newPoint(130, 130)
      )
    ), renderGeometry(feature));
  }

  @Test
  void testLabelGridRequiresBufferPixelsGreaterThanGridSize() {
    assertThrows(AssertionError.class, () -> renderFeatures(pointFeature(newPoint(0.75, 0.75))
      .setPointLabelGridSizeAndLimit(10, 10, 2)
      .setBufferPixels(9)));

    renderFeatures(pointFeature(newPoint(0.75, 0.75))
      .setPointLabelGridSizeAndLimit(10, 10, 2)
      .setBufferPixels(10));
  }

  @Test
  void testLabelGrid() {
    var feature = pointFeature(newPoint(0.75, 0.75))
      .setPointLabelGridSizeAndLimit(10, 256, 2)
      .setZoomRange(0, 1)
      .setBufferPixels(256);
    var rendered = renderFeatures(feature);
    var z0Feature = rendered.get(TileCoord.ofXYZ(0, 0, 0)).iterator().next();
    var z1Feature = rendered.get(TileCoord.ofXYZ(1, 1, 1)).iterator().next();
    assertEquals(Optional.of(new RenderedFeature.Group(0, 2)), z0Feature.group());
    assertEquals(Optional.of(new RenderedFeature.Group((1L << 32) + 1, 2)), z1Feature.group());
  }

  @Test
  void testWrapLabelGrid() {
    var feature = pointFeature(newPoint(1.1, -0.1))
      .setPointLabelGridSizeAndLimit(10, 256, 2)
      .setZoomRange(0, 1)
      .setBufferPixels(256);
    var rendered = renderFeatures(feature);
    var z0Feature = rendered.get(TileCoord.ofXYZ(0, 0, 0)).iterator().next();
    var z1Feature = rendered.get(TileCoord.ofXYZ(0, 0, 1)).iterator().next();
    assertEquals(Optional.of(new RenderedFeature.Group((1L << 32) - 1, 2)), z0Feature.group());
    assertEquals(Optional.of(new RenderedFeature.Group((1L << 32) - 1, 2)), z1Feature.group());
  }

  private FeatureCollector.Feature pointFeature(Geometry geom) {
    return collector(geom).point("layer");
  }

  /*
   * LINE TESTS
   */

  private FeatureCollector.Feature lineFeature(Geometry geom) {
    return collector(geom).line("layer");
  }

  @Test
  void testSplitLineFeatureSingleTile() {
    double z14hypot = Math.sqrt(Z14_WIDTH * Z14_WIDTH);
    var feature = lineFeature(newLineString(
      0.5 + z14hypot / 4, 0.5 + z14hypot / 4,
      0.5 + z14hypot * 3 / 4, 0.5 + z14hypot * 3 / 4
    ))
      .setZoomRange(14, 14)
      .setBufferPixels(8);
    assertExactSameFeatures(Map.of(
      TileCoord.ofXYZ(Z14_TILES / 2, Z14_TILES / 2, 14), List.of(
        newLineString(64, 64, 192, 192)
      )
    ), renderGeometry(feature));
  }

  @Test
  void testSimplifyLine() {
    double z14hypot = Math.sqrt(Z14_WIDTH * Z14_WIDTH);
    var feature = lineFeature(newLineString(
      0.5 + z14hypot / 4, 0.5 + z14hypot / 4,
      0.5 + z14hypot / 2, 0.5 + z14hypot / 2,
      0.5 + z14hypot * 3 / 4, 0.5 + z14hypot * 3 / 4
    ))
      .setZoomRange(14, 14)
      .setBufferPixels(8);
    assertExactSameFeatures(Map.of(
      TileCoord.ofXYZ(Z14_TILES / 2, Z14_TILES / 2, 14), List.of(
        newLineString(64, 64, 192, 192)
      )
    ), renderGeometry(feature));
  }

  @Test
  void testSplitLineFeatureTouchingNeighboringTile() {
    double z14hypot = Math.sqrt(Z14_WIDTH * Z14_WIDTH);
    var feature = lineFeature(newLineString(
      0.5 + z14hypot / 4, 0.5 + z14hypot / 4,
      0.5 + Z14_WIDTH * (256 - 8) / 256d, 0.5 + Z14_WIDTH * (256 - 8) / 256d
    ))
      .setZoomRange(14, 14)
      .setBufferPixels(8);
    assertExactSameFeatures(Map.of(
      TileCoord.ofXYZ(Z14_TILES / 2, Z14_TILES / 2, 14), List.of(
        newLineString(64, 64, 256 - 8, 256 - 8)
      )
    // only a single point in neighboring tile, exclude
    ), renderGeometry(feature));
  }

  @Test
  void testSplitLineFeatureEnteringNeighboringTileBoudary() {
    double z14hypot = Math.sqrt(Z14_WIDTH * Z14_WIDTH);
    var feature = lineFeature(newLineString(
      0.5 + z14hypot / 4, 0.5 + z14hypot / 4,
      0.5 + Z14_WIDTH * (256 - 7) / 256d, 0.5 + Z14_WIDTH * (256 - 7) / 256d
    ))
      .setZoomRange(14, 14)
      .setBufferPixels(8);
    assertExactSameFeatures(Map.of(
      TileCoord.ofXYZ(Z14_TILES / 2, Z14_TILES / 2, 14), List.of(
        newLineString(64, 64, 256 - 7, 256 - 7)
      ),
      TileCoord.ofXYZ(Z14_TILES / 2 + 1, Z14_TILES / 2, 14), List.of(
        newLineString(-8, 248, -7, 249)
      ),
      TileCoord.ofXYZ(Z14_TILES / 2, Z14_TILES / 2 + 1, 14), List.of(
        newLineString(248, -8, 249, -7)
      ),
      TileCoord.ofXYZ(Z14_TILES / 2 + 1, Z14_TILES / 2 + 1, 14), List.of(
        newLineString(-8, -8, -7, -7)
      )
    ), renderGeometry(feature));
  }

  @Test
  void test3PointLine() {
    var feature = lineFeature(newLineString(
      0.5 + Z14_WIDTH / 2, 0.5 + Z14_WIDTH / 2,
      0.5 + 3 * Z14_WIDTH / 2, 0.5 + Z14_WIDTH / 2,
      0.5 + 3 * Z14_WIDTH / 2, 0.5 + 3 * Z14_WIDTH / 2
    ))
      .setZoomRange(14, 14)
      .setBufferPixels(8);
    assertExactSameFeatures(Map.of(
      TileCoord.ofXYZ(Z14_TILES / 2, Z14_TILES / 2, 14), List.of(
        newLineString(128, 128, 256 + 8, 128)
      ),
      TileCoord.ofXYZ(Z14_TILES / 2 + 1, Z14_TILES / 2, 14), List.of(
        newLineString(-8, 128, 128, 128, 128, 256 + 8)
      ),
      TileCoord.ofXYZ(Z14_TILES / 2 + 1, Z14_TILES / 2 + 1, 14), List.of(
        newLineString(128, -8, 128, 128)
      )
    ), renderGeometry(feature));
  }

  @Test
  void testLimitSingleLineStringLength() {
    var eps = Z13_WIDTH / 4096;
    var pixel = Z13_WIDTH / 256;
    var featureBelow = lineFeature(newMultiLineString(
      // below limit - ignore
      newLineString(0.5, 0.5 + pixel, 0.5 + pixel - eps, 0.5 + pixel)
    ))
      .setMinPixelSize(1)
      .setZoomRange(13, 13)
      .setBufferPixels(0);
    var featureAbove = lineFeature(newMultiLineString(
      // above limit - allow
      newLineString(0.5, 0.5 + pixel, 0.5 + pixel + eps, 0.5 + pixel)
    ))
      .setMinPixelSize(1)
      .setZoomRange(13, 13)
      .setBufferPixels(0);
    assertExactSameFeatures(Map.of(), renderGeometry(featureBelow));
    assertExactSameFeatures(Map.of(
      TileCoord.ofXYZ(Z13_TILES / 2, Z13_TILES / 2, 13), List.of(
        newLineString(0, 1, 1 + 256d / 4096, 1)
      )
    ), renderGeometry(featureAbove));
  }

  @Test
  void testLimitMultiLineStringLength() {
    var eps = Z13_WIDTH / 4096;
    var pixel = Z13_WIDTH / 256;
    var feature = lineFeature(newMultiLineString(
      // below limit - ignore
      newLineString(0.5, 0.5 + pixel, 0.5 + pixel - eps, 0.5 + pixel),
      // above limit - allow
      newLineString(0.5, 0.5 + pixel, 0.5 + pixel + eps, 0.5 + pixel)
    ))
      .setMinPixelSize(1)
      .setZoomRange(13, 13)
      .setBufferPixels(0);
    assertExactSameFeatures(Map.of(
      TileCoord.ofXYZ(Z13_TILES / 2, Z13_TILES / 2, 13), List.of(
        newLineString(0, 1, 1 + 256d / 4096, 1)
      )
    ), renderGeometry(feature));
  }

  @Test
  void testDuplicatePointsRemovedAfterRounding() {
    var eps = Z14_WIDTH / 4096;
    var pixel = Z14_WIDTH / 256;
    var feature = lineFeature(newLineString(
      0.5 + pixel * 10, 0.5 + pixel * 10,
      0.5 + pixel * 20, 0.5 + pixel * 10,
      0.5 + pixel * 20, 0.5 + pixel * 10 + eps / 3
    ))
      .setMinPixelSize(1)
      .setZoomRange(14, 14)
      .setBufferPixels(0)
      .setPixelToleranceAtAllZooms(0);
    assertExactSameFeatures(Map.of(
      TileCoord.ofXYZ(Z14_TILES / 2, Z14_TILES / 2, 14), List.of(
        newLineString(
          10, 10,
          20, 10
        )
      )
    ), renderGeometry(feature));
  }

  @Test
  void testLineStringCollapsesToPointWithRounding() {
    var eps = Z14_WIDTH / 4096;
    var pixel = Z14_WIDTH / 256;
    var feature = lineFeature(newLineString(
      0.5 + pixel * 10, 0.5 + pixel * 10,
      0.5 + pixel * 10 + eps / 3, 0.5 + pixel * 10
    ))
      .setMinPixelSize(1)
      .setZoomRange(14, 14)
      .setBufferPixels(0)
      .setPixelToleranceAtAllZooms(0);
    assertExactSameFeatures(Map.of(), renderGeometry(feature));
  }

  @Test
  void testSelfIntersectingLineStringOK() {
    var feature = lineFeature(newLineString(z14WorldCoords(
      10, 10,
      20, 20,
      10, 20,
      20, 10,
      10, 10
    )))
      .setMinPixelSize(1)
      .setZoomRange(14, 14);
    assertExactSameFeatures(Map.of(
      TileCoord.ofXYZ(Z14_TILES / 2, Z14_TILES / 2, 14), List.of(newLineString(
        10, 10,
        20, 20,
        10, 20,
        20, 10,
        10, 10
      ))
    ), renderGeometry(feature));
  }

  @Test
  void testLineWrap() {
    var feature = lineFeature(newLineString(
      -1d / 256, -1d / 256,
      257d / 256, 257d / 256
    ))
      .setMinPixelSize(1)
      .setBufferPixels(4)
      .setZoomRange(0, 1);
    assertSameNormalizedFeatures(Map.of(
      TileCoord.ofXYZ(0, 0, 0), List.of(newMultiLineString(
        newLineString(
          -1, -1,
          257, 257
        ),
        newLineString(
          -4, 252,
          1, 257
        ),
        newLineString(
          255, -1,
          260, 4
        )
      )),
      TileCoord.ofXYZ(0, 0, 1), List.of(newLineString(
        -2, -2,
        260, 260
      )),
      TileCoord.ofXYZ(1, 0, 1), List.of(newMultiLineString(
        newLineString(
          -4, 252,
          4, 260
        ),
        newLineString(
          254, -2,
          260, 4
        )
      )),
      TileCoord.ofXYZ(0, 1, 1), List.of(newMultiLineString(
        newLineString(
          252, -4,
          260, 4
        ),
        newLineString(
          -4, 252,
          2, 258
        )
      )),
      TileCoord.ofXYZ(1, 1, 1), List.of(newLineString(
        -4, -4,
        258, 258
      ))
    ), renderGeometry(feature));
  }

  /*
   * POLYGON TESTS
   */
  private FeatureCollector.Feature polygonFeature(Geometry geom) {
    return collector(geom).polygon("layer");
  }

  @Test
  void testSimpleTriangleCCW() {
    var feature = polygonFeature(
      newPolygon(
        0.5 + Z14_PX * 10, 0.5 + Z14_PX * 10,
        0.5 + Z14_PX * 20, 0.5 + Z14_PX * 10,
        0.5 + Z14_PX * 10, 0.5 + Z14_PX * 20,
        0.5 + Z14_PX * 10, 0.5 + Z14_PX * 10
      )
    )
      .setMinPixelSize(1)
      .setZoomRange(14, 14)
      .setBufferPixels(0);
    assertSameNormalizedFeatures(Map.of(
      TileCoord.ofXYZ(Z14_TILES / 2, Z14_TILES / 2, 14), List.of(
        newPolygon(
          10, 10,
          20, 10,
          10, 20,
          10, 10
        )
      )
    ), renderGeometry(feature));
  }

  @Test
  void testSimpleTriangleCW() {
    var feature = polygonFeature(
      newPolygon(
        0.5 + Z14_PX * 10, 0.5 + Z14_PX * 10,
        0.5 + Z14_PX * 10, 0.5 + Z14_PX * 20,
        0.5 + Z14_PX * 20, 0.5 + Z14_PX * 10,
        0.5 + Z14_PX * 10, 0.5 + Z14_PX * 10
      )
    )
      .setMinPixelSize(1)
      .setZoomRange(14, 14)
      .setBufferPixels(0);
    assertSameNormalizedFeatures(Map.of(
      TileCoord.ofXYZ(Z14_TILES / 2, Z14_TILES / 2, 14), List.of(
        newPolygon(
          10, 10,
          10, 20,
          20, 10,
          10, 10
        )
      )
    ), renderGeometry(feature));
  }

  @Test
  void testTriangleTouchingNeighboringTileDoesNotEmit() {
    var feature = polygonFeature(
      newPolygon(
        0.5 + Z14_PX * 10, 0.5 + Z14_PX * 10,
        0.5 + Z14_PX * 256, 0.5 + Z14_PX * 10,
        0.5 + Z14_PX * 10, 0.5 + Z14_PX * 20,
        0.5 + Z14_PX * 10, 0.5 + Z14_PX * 10
      )
    )
      .setMinPixelSize(1)
      .setZoomRange(14, 14)
      .setBufferPixels(0);
    assertSameNormalizedFeatures(Map.of(
      TileCoord.ofXYZ(Z14_TILES / 2, Z14_TILES / 2, 14), List.of(
        newPolygon(
          10, 10,
          256, 10,
          10, 20,
          10, 10
        )
      )
    ), renderGeometry(feature));
  }

  @Test
  void testTriangleTouchingNeighboringTileBelowDoesNotEmit() {
    var feature = polygonFeature(
      newPolygon(
        0.5 + Z14_PX * 10, 0.5 + Z14_PX * 10,
        0.5 + Z14_PX * 20, 0.5 + Z14_PX * 10,
        0.5 + Z14_PX * 10, 0.5 + Z14_PX * 256,
        0.5 + Z14_PX * 10, 0.5 + Z14_PX * 10
      )
    )
      .setMinPixelSize(1)
      .setZoomRange(14, 14)
      .setBufferPixels(0);
    assertSameNormalizedFeatures(Map.of(
      TileCoord.ofXYZ(Z14_TILES / 2, Z14_TILES / 2, 14), List.of(
        newPolygon(
          10, 10,
          20, 10,
          10, 256,
          10, 10
        )
      )
    ), renderGeometry(feature));
  }

  @ParameterizedTest
  @CsvSource({
    "0,256, 0,256", // all

    "0,10, 0,10", // top-left
    "5,10, 0,10", // top partial
    "250,256, 0,10", // top all
    "250,256, 0,10", // top-right

    "250,256, 0,256", // right all
    "250,256, 10,250", // right partial
    "250,256, 250,256", // right bottom

    "0,256, 250,256", // bottom all
    "240,250, 250,256", // bottom partial
    "0,10, 250,256", // bottom left

    "0,10, 0,256", // left all
    "0,10, 240,250", // left partial
  })
  void testRectangleTouchingNeighboringTilesDoesNotEmit(int x1, int x2, int y1, int y2) {
    var feature = polygonFeature(
      rectangle(
        0.5 + Z14_PX * x1,
        0.5 + Z14_PX * y1,
        0.5 + Z14_PX * x2,
        0.5 + Z14_PX * y2
      )
    )
      .setMinPixelSize(1)
      .setZoomRange(14, 14)
      .setBufferPixels(0);
    assertSameNormalizedFeatures(Map.of(
      TileCoord.ofXYZ(Z14_TILES / 2, Z14_TILES / 2, 14), List.of(
        rectangle(x1, y1, x2, y2)
      )
    ), renderGeometry(feature));
  }

  @Test
  void testOverlapTileHorizontal() {
    var feature = polygonFeature(
      rectangle(
        0.5 + Z14_PX * 10,
        0.5 + Z14_PX * 10,
        0.5 + Z14_PX * 258,
        0.5 + Z14_PX * 20
      )
    )
      .setMinPixelSize(1)
      .setZoomRange(14, 14)
      .setBufferPixels(1);
    assertSameNormalizedFeatures(Map.of(
      TileCoord.ofXYZ(Z14_TILES / 2, Z14_TILES / 2, 14), List.of(
        rectangle(10, 10, 257, 20)
      ),
      TileCoord.ofXYZ(Z14_TILES / 2 + 1, Z14_TILES / 2, 14), List.of(
        rectangle(-1, 10, 2, 20)
      )
    ), renderGeometry(feature));
  }

  @Test
  void testOverlapTileVertical() {
    var feature = polygonFeature(
      rectangle(
        0.5 + Z14_PX * 10,
        0.5 + Z14_PX * 10,
        0.5 + Z14_PX * 20,
        0.5 + Z14_PX * 258
      )
    )
      .setMinPixelSize(1)
      .setZoomRange(14, 14)
      .setBufferPixels(1);
    assertSameNormalizedFeatures(Map.of(
      TileCoord.ofXYZ(Z14_TILES / 2, Z14_TILES / 2, 14), List.of(
        rectangle(10, 10, 20, 257)
      ),
      TileCoord.ofXYZ(Z14_TILES / 2, Z14_TILES / 2 + 1, 14), List.of(
        rectangle(10, -1, 20, 2)
      )
    ), renderGeometry(feature));
  }

  @Test
  void testOverlapTileCorner() {
    var feature = polygonFeature(
      rectangle(
        0.5 - Z14_PX * 10,
        0.5 - Z14_PX * 10,
        0.5 + Z14_PX * 10,
        0.5 + Z14_PX * 10
      )
    )
      .setMinPixelSize(1)
      .setZoomRange(14, 14)
      .setBufferPixels(1);
    assertSameNormalizedFeatures(Map.of(
      TileCoord.ofXYZ(Z14_TILES / 2 - 1, Z14_TILES / 2 - 1, 14), List.of(
        rectangle(246, 257)
      ),
      TileCoord.ofXYZ(Z14_TILES / 2, Z14_TILES / 2 - 1, 14), List.of(
        rectangle(-1, 246, 10, 257)
      ),
      TileCoord.ofXYZ(Z14_TILES / 2 - 1, Z14_TILES / 2, 14), List.of(
        rectangle(246, -1, 257, 10)
      ),
      TileCoord.ofXYZ(Z14_TILES / 2, Z14_TILES / 2, 14), List.of(
        rectangle(-1, 10)
      )
    ), renderGeometry(feature));
  }

  @Test
  void testFill() {
    var feature = polygonFeature(
      rectangle(
        0.5 - Z14_WIDTH / 2,
        0.5 + Z14_WIDTH * 3 / 2
      )
    )
      .setMinPixelSize(1)
      .setZoomRange(14, 14)
      .setBufferPixels(1);
    assertSameNormalizedFeatures(Map.of(
      // row 1
      TileCoord.ofXYZ(Z14_TILES / 2 - 1, Z14_TILES / 2 - 1, 14), List.of(
        tileBottomRight(1)
      ),
      TileCoord.ofXYZ(Z14_TILES / 2, Z14_TILES / 2 - 1, 14), List.of(
        tileBottom(1)
      ),
      TileCoord.ofXYZ(Z14_TILES / 2 + 1, Z14_TILES / 2 - 1, 14), List.of(
        tileBottomLeft(1)
      ),
      // row 2
      TileCoord.ofXYZ(Z14_TILES / 2 - 1, Z14_TILES / 2, 14), List.of(
        tileRight(1)
      ),
      TileCoord.ofXYZ(Z14_TILES / 2, Z14_TILES / 2, 14), List.of(
        newPolygon(tileFill(1), List.of()) // <<<<---- the filled tile!
      ),
      TileCoord.ofXYZ(Z14_TILES / 2 + 1, Z14_TILES / 2, 14), List.of(
        tileLeft(1)
      ),
      // row 1
      TileCoord.ofXYZ(Z14_TILES / 2 - 1, Z14_TILES / 2 + 1, 14), List.of(
        tileTopRight(1)
      ),
      TileCoord.ofXYZ(Z14_TILES / 2, Z14_TILES / 2 + 1, 14), List.of(
        tileTop(1)
      ),
      TileCoord.ofXYZ(Z14_TILES / 2 + 1, Z14_TILES / 2 + 1, 14), List.of(
        tileTopLeft(1)
      )
    ), renderGeometry(feature));
  }

  @Test
  void testWorldFill() {
    int maxZoom = 8;
    var feature = polygonFeature(rectangle(Z14_WIDTH / 2, 1 - Z14_WIDTH / 2))
      .setMinPixelSize(1)
      .setZoomRange(maxZoom, maxZoom)
      .setBufferPixels(0);
    AtomicLong num = new AtomicLong(0);
    new FeatureRenderer(config, rendered1 -> num.incrementAndGet(), Stats.inMemory())
      .accept(feature);
    assertEquals(num.get(), Math.pow(4, maxZoom));
  }

  @Test
  void testComplexPolygon() {
    var feature = polygonFeature(
      newPolygon(
        rectangleCoordList(0.5 + Z14_PX * 1, 0.5 + Z14_PX * 255),
        List.of(rectangleCoordList(0.5 + Z14_PX * 10, 0.5 + Z14_PX * 250))
      )
    )
      .setMinPixelSize(1)
      .setZoomRange(14, 14)
      .setBufferPixels(1);
    assertSameNormalizedFeatures(Map.of(
      TileCoord.ofXYZ(Z14_TILES / 2, Z14_TILES / 2, 14), List.of(
        newPolygon(
          rectangleCoordList(1, 255),
          List.of(rectangleCoordList(10, 250))
        )
      )
    ), renderGeometry(feature));
  }

  @Test
  void testComplexPolygonHoleInfersOuterFill() {
    var feature = polygonFeature(
      newPolygon(
        rectangleCoordList(0.5 - Z14_WIDTH / 2, 0.5 + 3 * Z14_WIDTH / 2),
        List.of(rectangleCoordList(0.5 + Z14_PX * 10, 0.5 + Z14_PX * 250))
      )
    )
      .setMinPixelSize(1)
      .setZoomRange(14, 14)
      .setBufferPixels(1);
    assertSameNormalizedFeatures(Map.of(
      // row 1
      TileCoord.ofXYZ(Z14_TILES / 2 - 1, Z14_TILES / 2 - 1, 14), List.of(
        tileBottomRight(1)
      ),
      TileCoord.ofXYZ(Z14_TILES / 2, Z14_TILES / 2 - 1, 14), List.of(
        tileBottom(1)
      ),
      TileCoord.ofXYZ(Z14_TILES / 2 + 1, Z14_TILES / 2 - 1, 14), List.of(
        tileBottomLeft(1)
      ),
      // row 2
      TileCoord.ofXYZ(Z14_TILES / 2 - 1, Z14_TILES / 2, 14), List.of(
        tileRight(1)
      ),
      TileCoord.ofXYZ(Z14_TILES / 2, Z14_TILES / 2, 14), List.of(
        // the filled tile with a hole!
        newPolygon(tileFill(1), List.of(rectangleCoordList(10, 250)))
      ),
      TileCoord.ofXYZ(Z14_TILES / 2 + 1, Z14_TILES / 2, 14), List.of(
        tileLeft(1)
      ),
      // row 1
      TileCoord.ofXYZ(Z14_TILES / 2 - 1, Z14_TILES / 2 + 1, 14), List.of(
        tileTopRight(1)
      ),
      TileCoord.ofXYZ(Z14_TILES / 2, Z14_TILES / 2 + 1, 14), List.of(
        tileTop(1)
      ),
      TileCoord.ofXYZ(Z14_TILES / 2 + 1, Z14_TILES / 2 + 1, 14), List.of(
        tileTopLeft(1)
      )
    ), renderGeometry(feature));
  }

  @Test
  void testComplexPolygonHoleBlocksFill() {
    var feature = polygonFeature(
      newPolygon(
        rectangleCoordList(0.5 - Z14_WIDTH / 2, 0.5 + 3 * Z14_WIDTH / 2),
        List.of(rectangleCoordList(0.5 - Z14_PX * 10, 0.5 + Z14_PX * 260))
      )
    )
      .setMinPixelSize(1)
      .setZoomRange(14, 14)
      .setBufferPixels(1);
    Map<TileCoord, Collection<Geometry>> rendered = renderGeometry(feature);
    // should be no data for center tile since it's inside the inner fill
    assertNull(rendered.get(TileCoord.ofXYZ(Z14_TILES / 2, Z14_TILES / 2, 14)));
    // notch taken out of bottom right
    assertEquals(
      List.of(new TestUtils.NormGeometry(newPolygon(
        128, 128,
        257, 128,
        257, 246,
        246, 246,
        246, 257,
        128, 257,
        128, 128
      ))),
      rendered.get(TileCoord.ofXYZ(Z14_TILES / 2 - 1, Z14_TILES / 2 - 1, 14)).stream()
        .map(TestUtils.NormGeometry::new)
        .toList()
    );
    // 4px taken out of top
    assertEquals(
      List.of(new TestUtils.NormGeometry(rectangle(-1, 4, 257, 128))),
      rendered.get(TileCoord.ofXYZ(Z14_TILES / 2, Z14_TILES / 2 + 1, 14)).stream()
        .map(TestUtils.NormGeometry::new)
        .toList()
    );
  }

  @Test
  void testMultipolygon() {
    var feature = polygonFeature(
      newMultiPolygon(
        rectangle(0.5 + Z14_PX * 10, 0.5 + Z14_PX * 20),
        rectangle(0.5 + Z14_PX * 30, 0.5 + Z14_PX * 40)
      )
    )
      .setMinPixelSize(1)
      .setZoomRange(14, 14)
      .setBufferPixels(1);
    assertSameNormalizedFeatures(Map.of(
      TileCoord.ofXYZ(Z14_TILES / 2, Z14_TILES / 2, 14), List.of(
        newMultiPolygon(
          rectangle(10, 20),
          rectangle(30, 40)
        )
      )
    ), renderGeometry(feature));
  }

  @Test
  void testFixInvalidInputGeometry() {
    var feature = polygonFeature(
      // bow tie
      newPolygon(
        0.5 + Z14_PX * 10, 0.5 + Z14_PX * 10,
        0.5 + Z14_PX * 30, 0.5 + Z14_PX * 10,
        0.5 + Z14_PX * 10, 0.5 + Z14_PX * 20,
        0.5 + Z14_PX * 20, 0.5 + Z14_PX * 20,
        0.5 + Z14_PX * 10, 0.5 + Z14_PX * 10
      )
    )
      .setMinPixelSize(1)
      .setZoomRange(14, 14)
      .setBufferPixels(1);
    assertSameNormalizedFeatures(Map.of(
      TileCoord.ofXYZ(Z14_TILES / 2, Z14_TILES / 2, 14), List.of(
        newPolygon(
          // it's not perfect, but at least it doesn't have a self-intersection
          10, 10,
          30, 10,
          16.6875, 16.6875,
          10, 10
        )
      )
    ), renderGeometry(feature));
  }

  @Test
  void testOmitsPolygonUnderMinSize() {
    var feature = polygonFeature(rectangle(0.5 + Z13_PX * 10, 0.5 + Z13_PX * 11.9))
      .setMinPixelSize(2)
      .setZoomRange(13, 13)
      .setBufferPixels(1);
    assertEquals(0, renderGeometry(feature).size());
    feature = polygonFeature(rectangle(0.5 + Z13_PX * 10, 0.5 + Z13_PX * 12.1))
      .setMinPixelSize(2)
      .setZoomRange(13, 13)
      .setBufferPixels(1);
    assertEquals(1, renderGeometry(feature).size());
  }

  @Test
  void testIncludesPolygonUnderMinTolerance() {
    var feature = polygonFeature(rectangle(0.5 + Z13_PX * 10, 0.5 + Z13_PX * 11.9))
      .setMinPixelSize(1)
      .setPixelTolerance(2)
      .setZoomRange(13, 13)
      .setBufferPixels(1);
    assertEquals(1, renderGeometry(feature).size());
  }

  @Test
  void testUses1pxMinAreaAtMaxZoom() {
    double base = 0.5 + Z14_WIDTH / 2;
    var feature = polygonFeature(rectangle(base, base + Z14_WIDTH / 4096 / 2))
      .setMinPixelSize(4)
      .setZoomRange(14, 14)
      .setBufferPixels(1);
    assertEquals(0, renderGeometry(feature).size());
    feature = polygonFeature(rectangle(base, base + 2 * Z14_WIDTH / 4096))
      .setMinPixelSize(4)
      .setZoomRange(14, 14)
      .setBufferPixels(1);
    assertEquals(1, renderGeometry(feature).size());
  }

  @Test
  void testRoundingCollapsesPolygonToLine() {
    var feature = polygonFeature(
      newPolygon(
        0.5 + Z14_PX * 10, 0.5 + Z14_PX * 10,
        0.5 + Z14_PX * (10 + 256d / 4096d / 2d), 0.5 + Z14_PX * 100,
        0.5 + Z14_PX * 10, 0.5 + Z14_PX * 200,
        0.5 + Z14_PX * 10, 0.5 + Z14_PX * 10
      )
    )
      .setMinPixelSize(1d / 4096)
      .setZoomRange(13, 13)
      .setBufferPixels(1);
    assertSameNormalizedFeatures(Map.of(), renderGeometry(feature));
  }

  private double[] z14WorldCoords(double... coords) {
    return DoubleStream.of(coords).map(c -> 0.5 + Z14_PX * c).toArray();
  }

  private static final double TILE_RESOLUTION_PX = 256d / 4096;

  @Test
  void testRoundingMakesOutputInvalid() {
    var feature = polygonFeature(
      newPolygon(z14WorldCoords(
        10, 10,
        10 + TILE_RESOLUTION_PX, 200,
        20, 200,
        10 + TILE_RESOLUTION_PX / 3d, 11,
        10, 10
      ))
    )
      .setMinPixelSize(1d / 4096)
      .setZoomRange(14, 14)
      .setBufferPixels(1);
    assertSameNormalizedFeatures(Map.of(
      TileCoord.ofXYZ(Z14_TILES / 2, Z14_TILES / 2, 14), List.of(
        newPolygon(
          // it's not perfect, but at least it doesn't have a self-intersection
          10, 10,
          20, 200,
          10.0625, 200,
          10, 10
        )
      )
    ), renderGeometry(feature));
  }

  @Test
  void testSimplifyMakesOutputInvalid() {
    var feature = polygonFeature(
      newPolygon(z14WorldCoords(
        10, 10,
        20, 20,
        10, 30,
        20 - TILE_RESOLUTION_PX / 0.5, 30,
        20 + TILE_RESOLUTION_PX / 10, 20,
        20 - TILE_RESOLUTION_PX / 0.5, 10,
        10, 10
      ))
    )
      .setMinPixelSize(1d / 4096)
      .setZoomRange(14, 14)
      .setBufferPixels(1);
    assertSameNormalizedFeatures(Map.of(
      TileCoord.ofXYZ(Z14_TILES / 2, Z14_TILES / 2, 14), List.of(
        newMultiPolygon(
          newPolygon(
            19.875, 10,
            20, 20,
            10, 10,
            19.875, 10
          ),
          newPolygon(
            10, 30,
            20, 20,
            19.875, 30,
            10, 30
          )
        )
      )
    ), renderGeometry(feature));
  }

  @Test
  void testNestedMultipolygon() {
    var feature = polygonFeature(
      newMultiPolygon(
        newPolygon(rectangleCoordList(
          0.5 + Z14_PX * 10,
          0.5 + Z14_PX * 200
        ), List.of(rectangleCoordList(
          0.5 + Z14_PX * 20,
          0.5 + Z14_PX * 190
        ))),
        rectangle(0.5 + Z14_PX * 30, 0.5 + Z14_PX * 180)
      )
    )
      .setMinPixelSize(1d / 4096)
      .setZoomRange(14, 14)
      .setBufferPixels(1);
    assertSameNormalizedFeatures(Map.of(
      TileCoord.ofXYZ(Z14_TILES / 2, Z14_TILES / 2, 14), List.of(
        newMultiPolygon(
          newPolygon(
            rectangleCoordList(10, 200),
            List.of(rectangleCoordList(20, 190))
          ),
          rectangle(30, 180)
        )
      )
    ), renderGeometry(feature));
  }

  @Test
  void testNestedMultipolygonFill() {
    var feature = polygonFeature(
      newMultiPolygon(
        newPolygon(rectangleCoordList(
          0.5 - Z14_PX * 30,
          0.5 + Z14_PX * (256 + 30)
        ), List.of(rectangleCoordList(
          0.5 - Z14_PX * 20,
          0.5 + Z14_PX * (256 + 20)
        ))),
        rectangle(0.5 - Z14_PX * 10, 0.5 + Z14_PX * (256 + 10))
      )
    )
      .setMinPixelSize(1d / 4096)
      .setZoomRange(14, 14)
      .setBufferPixels(1);
    var rendered = renderGeometry(feature);
    var innerTile = rendered.get(TileCoord.ofXYZ(Z14_TILES / 2, Z14_TILES / 2, 14));
    assertEquals(1, innerTile.size());
    assertEquals(new TestUtils.NormGeometry(rectangle(-1, 256 + 1)),
      new TestUtils.NormGeometry(innerTile.iterator().next()));
  }

  @Test
  void testNestedMultipolygonInfersOuterFill() {
    var feature = polygonFeature(
      newMultiPolygon(
        newPolygon(rectangleCoordList(
          0.5 - Z14_PX * 30,
          0.5 + Z14_PX * (256 + 30)
        ), List.of(rectangleCoordList(
          0.5 - Z14_PX * 20,
          0.5 + Z14_PX * (256 + 20)
        ))),
        newPolygon(rectangleCoordList(
          0.5 - Z14_PX * 10,
          0.5 + Z14_PX * (256 + 10)
        ), List.of(rectangleCoordList(
          0.5 + Z14_PX * 10,
          0.5 + Z14_PX * (256 - 10)
        )))
      )
    )
      .setMinPixelSize(1d / 4096)
      .setZoomRange(14, 14)
      .setBufferPixels(1);
    var rendered = renderGeometry(feature);
    var innerTile = rendered.get(TileCoord.ofXYZ(Z14_TILES / 2, Z14_TILES / 2, 14));
    assertEquals(1, innerTile.size());
    assertEquals(new TestUtils.NormGeometry(newPolygon(
      rectangleCoordList(-1, 256 + 1),
      List.of(rectangleCoordList(10, 246))
    )),
      new TestUtils.NormGeometry(innerTile.iterator().next()));
  }

  @Test
  void testNestedMultipolygonCancelsOutInnerFill() {
    var feature = polygonFeature(
      newMultiPolygon(
        newPolygon(rectangleCoordList(
          0.5 - Z14_PX * 30,
          0.5 + Z14_PX * (256 + 30)
        ), List.of(rectangleCoordList(
          0.5 - Z14_PX * 20,
          0.5 + Z14_PX * (256 + 20)
        ))),
        newPolygon(rectangleCoordList(
          0.5 - Z14_PX * 10,
          0.5 + Z14_PX * (256 + 10)
        ), List.of(rectangleCoordList(
          0.5 - Z14_PX * 5,
          0.5 + Z14_PX * (256 + 5)
        )))
      )
    )
      .setMinPixelSize(1d / 4096)
      .setZoomRange(14, 14)
      .setBufferPixels(1);
    var rendered = renderGeometry(feature);
    assertFalse(rendered.containsKey(TileCoord.ofXYZ(Z14_TILES / 2, Z14_TILES / 2, 14)));
  }

  @Test
  void testOverlappingMultipolygon() {
    var feature = polygonFeature(newMultiPolygon(
      rectangle(10d / 256, 10d / 256, 30d / 256, 30d / 256),
      rectangle(20d / 256, 20d / 256, 40d / 256, 40d / 256)
    ))
      .setMinPixelSize(1)
      .setBufferPixels(4)
      .setZoomRange(0, 0);
    assertSameNormalizedFeatures(Map.of(
      TileCoord.ofXYZ(0, 0, 0), List.of(newPolygon(
        10, 10,
        30, 10,
        30, 20,
        40, 20,
        40, 40,
        20, 40,
        20, 30,
        10, 30,
        10, 10
      ))
    ), renderGeometry(feature));
  }

  @Test
  void testOverlappingMultipolygonSideBySide() {
    var feature = polygonFeature(newMultiPolygon(
      rectangle(10d / 256, 10d / 256, 20d / 256, 20d / 256),
      rectangle(15d / 256, 10d / 256, 25d / 256, 20d / 256)
    ))
      .setMinPixelSize(1)
      .setBufferPixels(4)
      .setZoomRange(0, 0);
    assertTopologicallyEquivalentFeatures(Map.of(
      TileCoord.ofXYZ(0, 0, 0), List.of(rectangle(
        10, 10,
        25, 20
      ))
    ), renderGeometry(feature));
  }

  @Test
  void testPolygonWrap() {
    var feature = polygonFeature(rectangle(
      -1d / 256, -1d / 256, 257d / 256, 1d / 256
    ))
      .setMinPixelSize(1)
      .setBufferPixels(4)
      .setZoomRange(0, 1);
    assertTopologicallyEquivalentFeatures(Map.of(
      TileCoord.ofXYZ(0, 0, 0), List.of(
        rectangle(-4, -1, 260, 1)
      ),
      TileCoord.ofXYZ(0, 0, 1), List.of(
        rectangle(-4, -2, 260, 2)
      ),
      TileCoord.ofXYZ(1, 0, 1), List.of(
        rectangle(-4, -2, 260, 2)
      )
    ), renderGeometry(feature));
  }

  private static Geometry rotateWorld(Geometry geom, double degrees) {
    return AffineTransformation.rotationInstance(-Math.PI * degrees / 180, 0.5 + Z14_WIDTH / 2, 0.5 + Z14_WIDTH / 2)
      .transform(geom);
  }

  private static Geometry rotateTile(Geometry geom, double degrees) {
    return AffineTransformation.rotationInstance(-Math.PI * degrees / 180, 128, 128)
      .transform(geom);
  }

  private void testClipWithRotation(double rotation, Geometry inputTile) {
    Geometry input = new AffineTransformation()
      .scale(1d / 256 / Z14_TILES, 1d / 256 / Z14_TILES)
      .translate(0.5, 0.5)
      .transform(inputTile);
    Geometry expectedOutput = inputTile.intersection(rectangle(-4, 260));

    var feature = polygonFeature(rotateWorld(input, rotation))
      .setBufferPixels(4)
      .setZoomRange(14, 14);
    var geom = renderGeometry(feature)
      .get(TileCoord.ofXYZ(Z14_TILES / 2, Z14_TILES / 2, 14))
      .iterator().next();

    assertTopologicallyEquivalentFeature(
      round(rotateTile(expectedOutput, rotation)),
      round(geom)
    );
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 90, 180, -90})
  void testBackAndForthsOutsideTile(int rotation) {
    testClipWithRotation(rotation, newPolygon(
      300, -10,
      310, 300,
      320, -10,
      330, 300,
      340, 400,
      128, 400,
      128, 128,
      128, -10,
      300, -10
    ));
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 90, 180, -90})
  void testReplayEdgesOuterPoly(int rotation) {
    testClipWithRotation(rotation, newPolygon(
      130, -10,
      270, -10,
      270, 270,
      -10, 270,
      -10, -10,
      120, -10,
      120, 10,
      130, 10,
      130, -10
    ));
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 90, 180, -90})
  void testReplayEdgesInnerPoly(int rotation) {
    var innerShape = newCoordinateList(
      130, -10,
      270, -10,
      270, 270,
      -10, 270,
      -10, -10,
      120, -10,
      120, 10,
      130, 10,
      130, -10
    );
    Collections.reverse(innerShape);
    testClipWithRotation(rotation, newPolygon(
      rectangleCoordList(-20, 300),
      List.of(innerShape)
    ));
  }

  @ParameterizedTest
  @CsvSource({
    "0, 0",
    "0.5, 0",
    "0.5, 0.5",
    "0, 0.5",
    "-0.5, 0.5",
    "-0.5, 0",
    "-0.5, -0.5",
    "0, -0.5",
    "0.5, -0.5"
  })
  void testSpiral(double dx, double dy) {
    // generate spirals at different offsets and make sure that tile clipping
    // returns the same result as JTS intersection with the tile's boundary
    List<Coordinate> coords = new ArrayList<>();
    int outerRadius = 300;
    int iters = 25;
    for (int i = 0; i < iters; i++) {
      int radius = outerRadius - i * 10;
      coords.add(new CoordinateXY(-radius, 0));
      coords.add(new CoordinateXY(0, -radius));
      coords.add(new CoordinateXY(radius, 0));
      coords.add(new CoordinateXY(0, radius));
    }
    Geometry poly = newLineString(coords).buffer(1, 1);
    poly = AffineTransformation.translationInstance(128 + dx * 256, 128 + dy * 256).transform(poly);

    Geometry input = new AffineTransformation()
      .scale(1d / 256d / Z14_TILES, 1d / 256d / Z14_TILES)
      .translate(0.5, 0.5)
      .transform(poly);
    Geometry expectedOutput = poly.intersection(rectangle(-4, 260));

    var feature = polygonFeature(input)
      .setBufferPixels(4)
      .setZoomRange(14, 14);
    var actual = renderGeometry(feature)
      .get(TileCoord.ofXYZ(Z14_TILES / 2, Z14_TILES / 2, 14))
      .iterator().next();

    assertTopologicallyEquivalentFeature(
      GeometryPrecisionReducer.reduce(expectedOutput, new PrecisionModel(4096d / 256d)),
      actual
    );
  }

  @Test
  void testLinearRangeFeature() {
    var feature = lineFeature(
      newLineString(
        0.5 + Z14_WIDTH / 2, 0.5 + Z14_WIDTH / 2,
        0.5 + Z14_WIDTH / 2 + Z14_PX * 10, 0.5 + Z14_WIDTH / 2 + Z14_PX * 10
      )
    ).linearRange(0.5, 1).setAttr("k", "v").entireLine();
    Map<TileCoord, Collection<RenderedFeature>> rendered = renderFeatures(feature);
    assertEquals(
      Set.of(
        List.of(newLineString(128 + 5, 128 + 5, 128 + 10, 128 + 10), Map.of("k", "v")),
        List.of(newLineString(128, 128, 128 + 5, 128 + 5), Map.of())
      ),
      rendered.get(TileCoord.ofXYZ(Z14_TILES / 2, Z14_TILES / 2, 14)).stream()
        .map(RenderedFeature::vectorTileFeature)
        .map(d -> {
          try {
            return List.of(d.geometry().decode(), d.tags());
          } catch (GeometryException e) {
            throw new RuntimeException(e);
          }
        })
        .collect(Collectors.toSet())
    );
  }


  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testLinearRangeFeaturePartialMinzoom(boolean viaMinzoom) {
    var feature = lineFeature(
      newLineString(
        0.5 + Z13_WIDTH / 2, 0.5 + Z13_WIDTH / 2,
        0.5 + Z13_WIDTH / 2 + Z13_PX * 10, 0.5 + Z13_WIDTH / 2 + Z13_PX * 10
      )
    );
    if (viaMinzoom) {
      feature.linearRange(0.5, 1).setMinZoom(14);
    } else {
      feature.linearRange(0.5, 1).omit();
    }
    Map<TileCoord, Collection<RenderedFeature>> rendered = renderFeatures(feature);
    assertEquals(
      Set.of(
        List.of(newLineString(128, 128, 128 + 5, 128 + 5), Map.of())
      ),
      rendered.get(TileCoord.ofXYZ(Z13_TILES / 2, Z13_TILES / 2, 13)).stream()
        .map(RenderedFeature::vectorTileFeature)
        .map(d -> {
          try {
            return List.of(d.geometry().decode(), d.tags());
          } catch (GeometryException e) {
            throw new RuntimeException(e);
          }
        })
        .collect(Collectors.toSet())
    );
  }

  @Test
  void testGeometryPipeline() {
    var feature = lineFeature(
      newLineString(
        0.5 + Z14_WIDTH / 2, 0.5 + Z14_WIDTH / 2,
        0.5 + Z14_WIDTH / 2 + Z14_PX * 10, 0.5 + Z14_WIDTH / 2
      )
    ).setGeometryTransform(Geometry::getCentroid).setAttr("k", "v");
    assertEquals(
      Set.of(
        List.of(newPoint(128 + 5, 128), Map.of("k", "v"))
      ),
      renderedTileFeatures(feature, TileCoord.ofXYZ(Z14_TILES / 2, Z14_TILES / 2, 14))
    );
  }

  @Test
  void testGeometryPipelineSimplify() {
    var feature = lineFeature(
      newLineString(
        0.5 + Z14_WIDTH / 2, 0.5 + Z14_WIDTH / 2,
        0.5 + Z14_WIDTH / 2 + Z14_PX * 10, 0.5 + Z14_WIDTH / 2
      )
    ).setGeometryTransform(
      GeometryPipeline.simplifyVW(1).setWeight(0.9)
        .andThen(GeometryPipeline.simplifyDP(1))
    ).setAttr("k", "v");
    assertEquals(
      Set.of(
        List.of(newLineString(128, 128, 128 + 10, 128), Map.of("k", "v"))
      ),
      renderedTileFeatures(feature, TileCoord.ofXYZ(Z14_TILES / 2, Z14_TILES / 2, 14))
    );
  }
}
