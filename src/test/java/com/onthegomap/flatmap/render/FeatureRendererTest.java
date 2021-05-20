package com.onthegomap.flatmap.render;

import static com.onthegomap.flatmap.TestUtils.assertSameNormalizedFeatures;
import static com.onthegomap.flatmap.TestUtils.emptyGeometry;
import static com.onthegomap.flatmap.TestUtils.newLineString;
import static com.onthegomap.flatmap.TestUtils.newMultiPoint;
import static com.onthegomap.flatmap.TestUtils.newPoint;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import com.onthegomap.flatmap.CommonParams;
import com.onthegomap.flatmap.FeatureCollector;
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
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.locationtech.jts.geom.Geometry;

public class FeatureRendererTest {

  private final CommonParams config = CommonParams.defaults();

  private FeatureCollector collector(Geometry worldGeom) {
    var latLonGeom = GeoUtils.worldToLatLonCoords(worldGeom);
    return new FeatureCollector.Factory(config).get(new ReaderFeature(latLonGeom, 0));
  }

  private Map<TileCoord, Collection<Geometry>> renderGeometry(FeatureCollector.Feature feature) {
    Map<TileCoord, Collection<Geometry>> result = new TreeMap<>();
    new FeatureRenderer(config, rendered -> result.computeIfAbsent(rendered.tile(), tile -> new HashSet<>())
      .add(rendered.vectorTileFeature().geometry().decode())).renderFeature(feature);
    return result;
  }

  private Map<TileCoord, Collection<RenderedFeature>> renderFeatures(FeatureCollector.Feature feature) {
    Map<TileCoord, Collection<RenderedFeature>> result = new TreeMap<>();
    new FeatureRenderer(config, rendered -> result.computeIfAbsent(rendered.tile(), tile -> new HashSet<>())
      .add(rendered)).renderFeature(feature);
    return result;
  }

  private static final int Z14_TILES = 1 << 14;
  private static final double Z14_WIDTH = 1d / Z14_TILES;

  @Test
  public void testEmptyGeometry() {
    var feature = collector(emptyGeometry()).point("layer");
    assertSameNormalizedFeatures(Map.of(), renderGeometry(feature));
  }

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
    assertSameNormalizedFeatures(Map.of(
      TileCoord.ofXYZ(Z14_TILES / 2, Z14_TILES / 2, 14), List.of(
        newPoint(64, 64),
        newPoint(192, 192)
      )
    ), renderGeometry(feature));
  }

  // TODO: centroid
  // TODO: line
  // TODO: multilinestring
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
