package com.onthegomap.flatmap.render;

import static com.onthegomap.flatmap.TestUtils.assertExactSameFeatures;
import static com.onthegomap.flatmap.TestUtils.assertSameNormalizedFeatures;
import static com.onthegomap.flatmap.TestUtils.emptyGeometry;
import static com.onthegomap.flatmap.TestUtils.newLineString;
import static com.onthegomap.flatmap.TestUtils.newMultiLineString;
import static com.onthegomap.flatmap.TestUtils.newMultiPoint;
import static com.onthegomap.flatmap.TestUtils.newMultiPolygon;
import static com.onthegomap.flatmap.TestUtils.newPoint;
import static com.onthegomap.flatmap.TestUtils.newPolygon;
import static com.onthegomap.flatmap.TestUtils.rectangle;
import static com.onthegomap.flatmap.TestUtils.rectangleCoordList;
import static com.onthegomap.flatmap.TestUtils.tileBottom;
import static com.onthegomap.flatmap.TestUtils.tileBottomLeft;
import static com.onthegomap.flatmap.TestUtils.tileBottomRight;
import static com.onthegomap.flatmap.TestUtils.tileFill;
import static com.onthegomap.flatmap.TestUtils.tileLeft;
import static com.onthegomap.flatmap.TestUtils.tileRight;
import static com.onthegomap.flatmap.TestUtils.tileTop;
import static com.onthegomap.flatmap.TestUtils.tileTopLeft;
import static com.onthegomap.flatmap.TestUtils.tileTopRight;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import com.onthegomap.flatmap.Arguments;
import com.onthegomap.flatmap.CommonParams;
import com.onthegomap.flatmap.FeatureCollector;
import com.onthegomap.flatmap.TestUtils;
import com.onthegomap.flatmap.geo.GeoUtils;
import com.onthegomap.flatmap.geo.TileCoord;
import com.onthegomap.flatmap.read.ReaderFeature;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.locationtech.jts.geom.Geometry;

public class FeatureRendererTest {

  private CommonParams config = CommonParams.defaults();

  private FeatureCollector collector(Geometry worldGeom) {
    var latLonGeom = GeoUtils.worldToLatLonCoords(worldGeom);
    return new FeatureCollector.Factory(config).get(new ReaderFeature(latLonGeom, 0));
  }

  private Map<TileCoord, Collection<Geometry>> renderGeometry(FeatureCollector.Feature feature) {
    Map<TileCoord, Collection<Geometry>> result = new TreeMap<>();
    new FeatureRenderer(config, rendered -> result.computeIfAbsent(rendered.tile(), tile -> new HashSet<>())
      .add(rendered.vectorTileFeature().geometry().decode())).renderFeature(feature);
    result.values().forEach(gs -> gs.forEach(TestUtils::validateGeometry));
    return result;
  }

  private Map<TileCoord, Collection<RenderedFeature>> renderFeatures(FeatureCollector.Feature feature) {
    Map<TileCoord, Collection<RenderedFeature>> result = new TreeMap<>();
    new FeatureRenderer(config, rendered -> result.computeIfAbsent(rendered.tile(), tile -> new HashSet<>())
      .add(rendered)).renderFeature(feature);
    result.values()
      .forEach(gs -> gs.forEach(f -> TestUtils.validateGeometry(f.vectorTileFeature().geometry().decode())));
    return result;
  }

  private static final int Z14_TILES = 1 << 14;
  private static final double Z14_WIDTH = 1d / Z14_TILES;
  private static final double Z14_PX = Z14_WIDTH / 256;
  private static final int Z13_TILES = 1 << 13;
  private static final double Z13_WIDTH = 1d / Z13_TILES;
  private static final double Z13_PX = Z13_WIDTH / 256;

  @Test
  public void testEmptyGeometry() {
    var feature = collector(emptyGeometry()).point("layer");
    assertSameNormalizedFeatures(Map.of(), renderGeometry(feature));
  }

  /*
   * POINT TESTS
   */

  @Test
  public void testSinglePoint() {
    var feature = pointFeature(newPoint(0.5 + Z14_WIDTH / 2, 0.5 + Z14_WIDTH / 2))
      .setZoomRange(14, 14);
    assertSameNormalizedFeatures(Map.of(
      TileCoord.ofXYZ(Z14_TILES / 2, Z14_TILES / 2, 14), List.of(
        newPoint(128, 128)
      )
    ), renderGeometry(feature));
  }

  @Test
  public void testRepeatSinglePointNeighboringTiles() {
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
  public void testRepeatSinglePointNeighboringTilesBuffer0() {
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
  public void testEmitPointsRespectExtents() {
    config = CommonParams.from(Arguments.of(
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

  @TestFactory
  public List<DynamicTest> testProcessPointsNearInternationalDateLineAndPoles() {
    double d = 1d / 512;
    record X(double x, double wrapped, double z1x0, double z1x1) {

    }
    record Y(double y, int z1ty, double tyoff) {

    }
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
  public void testZ0FullTileBuffer() {
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
  public void testMultipointNoLabelGrid() {
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
  public void testMultipointWithLabelGridSplits() {
    var feature = pointFeature(newMultiPoint(
      newPoint(0.25, 0.25),
      newPoint(0.25 + 1d / 256, 0.25 + 1d / 256)
    ))
      .setLabelGridPixelSize(10, 256)
      .setZoomRange(0, 1)
      .setBufferPixels(4);
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
  public void testLabelGrid() {
    var feature = pointFeature(newPoint(0.75, 0.75))
      .setLabelGridSizeAndLimit(10, 256, 2)
      .setZoomRange(0, 1)
      .setBufferPixels(4);
    var rendered = renderFeatures(feature);
    var z0Feature = rendered.get(TileCoord.ofXYZ(0, 0, 0)).iterator().next();
    var z1Feature = rendered.get(TileCoord.ofXYZ(1, 1, 1)).iterator().next();
    assertEquals(Optional.of(new RenderedFeature.Group(0, 2)), z0Feature.group());
    assertEquals(Optional.of(new RenderedFeature.Group((1L << 32) + 1, 2)), z1Feature.group());
  }

  @Test
  public void testWrapLabelGrid() {
    var feature = pointFeature(newPoint(1.1, -0.1))
      .setLabelGridSizeAndLimit(10, 256, 2)
      .setZoomRange(0, 1)
      .setBufferPixels(64);
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
  public void testSplitLineFeatureSingleTile() {
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
  public void testSimplifyLine() {
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
  public void testSplitLineFeatureTouchingNeighboringTile() {
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
  public void testSplitLineFeatureEnteringNeighboringTileBoudary() {
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
  public void test3PointLine() {
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
  public void testLimitSingleLineStringLength() {
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
  public void testLimitMultiLineStringLength() {
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

  /*
   * POLYGON TESTS
   */
  private FeatureCollector.Feature polygonFeature(Geometry geom) {
    return collector(geom).polygon("layer");
  }

  @Test
  public void testSimpleTriangleCCW() {
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
  public void testSimpleTriangleCW() {
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
  public void testTriangleTouchingNeighboringTileDoesNotEmit() {
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
  public void testTriangleTouchingNeighboringTileBelowDoesNotEmit() {
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
  public void testRectangleTouchingNeighboringTilesDoesNotEmit(int x1, int x2, int y1, int y2) {
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
  public void testOverlapTileHorizontal() {
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
  public void testOverlapTileVertical() {
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
  public void testOverlapTileCorner() {
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
  public void testFill() {
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
        newPolygon(tileFill(5), List.of()) // <<<<---- the filled tile!
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
  public void testWorldFill() {
    int maxZoom = 8;
    var feature = polygonFeature(rectangle(Z14_WIDTH / 2, 1 - Z14_WIDTH / 2))
      .setMinPixelSize(1)
      .setZoomRange(maxZoom, maxZoom)
      .setBufferPixels(0);
    AtomicLong num = new AtomicLong(0);
    new FeatureRenderer(config, rendered1 -> num.incrementAndGet())
      .renderFeature(feature);
    assertEquals(num.get(), Math.pow(4, maxZoom));
  }

  @Test
  public void testComplexPolygon() {
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
  public void testComplexPolygonHoleInfersOuterFill() {
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
        newPolygon(tileFill(1 + 256d / 4096), List.of(rectangleCoordList(10, 250)))
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
  public void testComplexPolygonHoleBlocksFill() {
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
  public void testMultipolygon() {
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
  public void testFixInvalidInputGeometry() {
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
  public void testOmitsPolygonUnderMinSize() {
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
  public void testUses1pxMinAreaAtMaxZoom() {
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
  public void testRoundingMakesOutputWrongWinding() {
    var feature = polygonFeature(
      newPolygon(
        0.5 + Z13_PX * 10, 0.5 + Z13_PX * 10,
        0.5 + Z13_PX * (10 + (256d / 4096 / 3)), 0.5 + Z13_PX * 11,
        0.5 + Z13_PX * (10 + (256d / 4096)), 0.5 + Z13_PX * 200,
        0.5 + Z13_PX * 10, 0.5 + Z13_PX * 10
      )
    )
      .setMinPixelSize(1d / 4096)
      .setZoomRange(13, 13)
      .setBufferPixels(1);
    assertSameNormalizedFeatures(Map.of(
      TileCoord.ofXYZ(Z13_TILES / 2, Z13_TILES / 2, 14), List.of(
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
  public void testSimplifyMakesOutputInvalid() {
    // simplifying causes invalid
    throw new Error("TODO");
  }

  @Test
  public void testRoundingMakesOutputInvalid() {
    // simplifying causes invalid
    throw new Error("TODO");
  }

  // TODO: centroid
  // TODO: poly
  // TODO: multipolygon
  // TODO: geometry collection

  // sad tests:
  // TODO: invalid line
  // TODO: invalid poly
  // TODO: coerce poly -> line
  // TODO: coerce line -> poly
  // TODO: wrong types: point/line/poly -> point/line/poly
}
