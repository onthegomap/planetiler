package com.onthegomap.planetiler;

import static com.onthegomap.planetiler.geo.GeoUtils.JTS_FACTORY;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.geo.GeometryPipeline;
import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.reader.SimpleFeature;
import com.onthegomap.planetiler.render.FeatureRenderer;
import com.onthegomap.planetiler.render.RenderedFeature;
import com.onthegomap.planetiler.stats.Stats;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.locationtech.jts.algorithm.Orientation;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;
import vector_tile.VectorTileProto;

class RendererPolygonLimitTest {

  private static final TileCoord Z0 = TileCoord.ofXYZ(0, 0, 0);

  @AfterEach
  void restoreExtent() {
    VectorTile.setExtent(VectorTile.DEFAULT_EXTENT);
    GeoUtils.setTileExtent(VectorTile.DEFAULT_EXTENT);
  }

  @Test
  void passing59999VertexRingRemainsUnchanged() {
    VectorTile.setExtent(1 << 20);
    Polygon polygon = polygon(noisyCircle(59_999, 80, 2));
    VectorTile.VectorGeometry geometry = VectorTile.encodeGeometry(polygon);
    assertEquals(59_999, VectorTile.rendererPolygonVertexCount(geometry.commands()));

    int[] result = enforceAndSerialize(geometry, 60_000, 256);

    assertArrayEquals(geometry.commands(), result);
  }

  @Test
  void failing60001VertexRingIsRepairedAndValid() throws GeometryException {
    VectorTile.setExtent(1 << 20);
    VectorTile.VectorGeometry geometry = VectorTile.encodeGeometry(polygon(noisyCircle(60_001, 80, 2)));
    assertEquals(60_001, VectorTile.rendererPolygonVertexCount(geometry.commands()));

    int[] repaired = enforceAndSerialize(geometry, 60_000, 256);

    assertTrue(VectorTile.rendererPolygonVertexCount(repaired) <= 60_000);
    assertNotEquals(Arrays.hashCode(geometry.commands()), Arrays.hashCode(repaired));
    assertTrue(new VectorTile.VectorGeometry(repaired, geometry.geomType(), 0).decode().isValid());
  }

  @Test
  void ringAboveMapLibre65535LimitIsRepaired() {
    VectorTile.setExtent(1 << 20);
    VectorTile.VectorGeometry geometry = VectorTile.encodeGeometry(polygon(noisyCircle(66_000, 80, 2)));
    assertTrue(VectorTile.rendererPolygonVertexCount(geometry.commands()) > 65_535);

    int[] repaired = enforceAndSerialize(geometry, 60_000, 256);

    assertTrue(VectorTile.rendererPolygonVertexCount(repaired) <= 60_000);
  }

  @Test
  void multipolygonIsCheckedPerComponentRatherThanByFeatureTotal() {
    VectorTile.setExtent(1 << 20);
    Polygon left = polygon(noisyCircle(35_000, 35, 1, 60, 128));
    Polygon right = polygon(noisyCircle(35_000, 35, 1, 196, 128));
    VectorTile.VectorGeometry geometry = VectorTile.encodeGeometry(
      JTS_FACTORY.createMultiPolygon(new Polygon[]{left, right}));
    assertTrue(left.getNumPoints() + right.getNumPoints() > 60_000);
    assertEquals(35_000, VectorTile.rendererPolygonVertexCount(geometry.commands()));

    assertArrayEquals(geometry.commands(), enforceAndSerialize(geometry, 60_000, 256));
  }

  @Test
  void multipolygonWithOversizedComponentIsRepaired() throws GeometryException {
    VectorTile.setExtent(1 << 20);
    Polygon large = polygon(noisyCircle(60_001, 70, 2, 90, 128));
    Polygon small = polygon(noisyCircle(100, 15, 1, 220, 128));
    VectorTile.VectorGeometry geometry = VectorTile.encodeGeometry(
      JTS_FACTORY.createMultiPolygon(new Polygon[]{large, small}));
    assertEquals(60_001, VectorTile.rendererPolygonVertexCount(geometry.commands()));

    int[] repaired = enforceAndSerialize(geometry, 60_000, 256);

    assertTrue(VectorTile.rendererPolygonVertexCount(repaired) <= 60_000);
    assertTrue(new VectorTile.VectorGeometry(repaired, geometry.geomType(), 0).decode().isValid());
  }

  @Test
  void exactly500HolesAreCounted() {
    VectorTile.setExtent(8192);
    VectorTile.VectorGeometry geometry = VectorTile.encodeGeometry(polygonWithHoles(500, false));

    assertEquals(4 + 500 * 4, VectorTile.rendererPolygonVertexCount(geometry.commands()));
  }

  @Test
  void only500LargestHolesAreCounted() {
    VectorTile.setExtent(8192);
    VectorTile.VectorGeometry geometry = VectorTile.encodeGeometry(polygonWithHoles(501, true));

    // The 501st and smallest hole has three vertices and is deliberately excluded.
    assertEquals(4 + 500 * 4, VectorTile.rendererPolygonVertexCount(geometry.commands()));
  }

  @Test
  void polygonWithManyDetailedHolesIsRepaired() throws GeometryException {
    VectorTile.setExtent(1 << 20);
    VectorTile.VectorGeometry geometry = VectorTile.encodeGeometry(detailedPolygonWithHoles(500, 125));
    assertTrue(VectorTile.rendererPolygonVertexCount(geometry.commands()) > 60_000);

    int[] repaired = enforceAndSerialize(geometry, 60_000, 256);

    assertTrue(VectorTile.rendererPolygonVertexCount(repaired) <= 60_000);
    assertTrue(new VectorTile.VectorGeometry(repaired, geometry.geomType(), 0).decode().isValid());
  }

  @Test
  void ringFallbackPreservesMvtWinding() {
    Coordinate[] coordinates = noisyCircle(1_000, 80, 2);
    for (boolean reverse : List.of(false, true)) {
      Coordinate[] oriented = Arrays.stream(coordinates).map(Coordinate::copy).toArray(Coordinate[]::new);
      if (reverse) {
        reverse(oriented);
      }
      LinearRing ring = JTS_FACTORY.createLinearRing(oriented);
      LinearRing simplified = VectorTile.simplifyRingForRendererLimit(ring, 1, JTS_FACTORY);

      assertEquals(Orientation.isCCW(ring.getCoordinateSequence()),
        Orientation.isCCW(simplified.getCoordinateSequence()));
    }
  }

  @Test
  void repairedCandidateNormalizesShellAndHoleWindingForMvt() throws GeometryException {
    Polygon first = polygonWithHoles(1, false);
    Polygon second = polygon(noisyCircle(100, 20, 1, 300, 128));
    // Deliberately give the second shell the winding that MVT reserves for holes.
    second = JTS_FACTORY.createPolygon((LinearRing) second.getExteriorRing().reverse());
    Geometry normalized = VectorTile.normalizeRendererPolygonWinding(
      JTS_FACTORY.createMultiPolygon(new Polygon[]{first, second}));

    for (int i = 0; i < normalized.getNumGeometries(); i++) {
      Polygon polygon = (Polygon) normalized.getGeometryN(i);
      assertTrue(Orientation.isCCW(polygon.getExteriorRing().getCoordinateSequence()));
      for (int j = 0; j < polygon.getNumInteriorRing(); j++) {
        assertTrue(!Orientation.isCCW(polygon.getInteriorRingN(j).getCoordinateSequence()));
      }
    }
    assertTrue(VectorTile.encodeGeometry(normalized).decode().isValid());
  }

  @Test
  void invalidFallbackMultipolygonIsFixedBeforeEncoding() throws GeometryException {
    Polygon left = polygon(closedRing(
      new Coordinate(0, 0), new Coordinate(20, 0), new Coordinate(20, 20), new Coordinate(0, 20)));
    Polygon overlapping = polygon(closedRing(
      new Coordinate(10, 10), new Coordinate(30, 10), new Coordinate(30, 30), new Coordinate(10, 30)));
    MultiPolygon invalid = JTS_FACTORY.createMultiPolygon(new Polygon[]{left, overlapping});
    assertTrue(!invalid.isValid());

    Geometry repaired = VectorTile.repairRendererPolygonTopology(invalid);

    assertTrue(!repaired.isEmpty());
    assertTrue(repaired instanceof Polygon || repaired instanceof MultiPolygon);
    assertTrue(repaired.isValid());
    assertTrue(VectorTile.encodeGeometry(VectorTile.normalizeRendererPolygonWinding(repaired)).decode().isValid());
  }

  @Test
  void zeroAreaAndDegenerateRingsAreIgnored() {
    Coordinate[] outer = {
      new Coordinate(0, 0), new Coordinate(100, 0), new Coordinate(100, 100), new Coordinate(0, 100)
    };
    Coordinate[] zeroArea = {
      new Coordinate(10, 10), new Coordinate(20, 20), new Coordinate(30, 30), new Coordinate(20, 20)
    };

    assertEquals(4, VectorTile.rendererPolygonVertexCount(encodeRawRings(outer, zeroArea)));
  }

  @ParameterizedTest
  @ValueSource(ints = {4096, 8192})
  void actualClipQuantizeEncodeEnforceAndSerializePath(int extent) throws GeometryException {
    // This input starts in world coordinates and passes through FeatureRenderer clipping, polygon repair,
    // quantization, MVT command encoding, renderer-limit enforcement, and protobuf serialization.
    Polygon worldPolygon = polygon(sawtoothCoastline(3_500, 20));
    RenderPathResult rendered = renderFinalGeometry(worldPolygon, extent);
    assertTrue(rendered.originalCount() > 60_000,
      "synthetic ring retained " + rendered.originalCount() +
        " vertices after clipping and quantization at extent " + extent);

    int[] repaired = enforceAndSerialize(rendered.geometry(), 60_000, 256);

    assertTrue(VectorTile.rendererPolygonVertexCount(repaired) <= 60_000);
    assertTrue(new VectorTile.VectorGeometry(repaired, rendered.geometry().geomType(), 0).decode().isValid());
  }

  @Test
  void difficultGeometryFailsAtMaximumTolerance() {
    VectorTile.setExtent(1 << 20);
    VectorTile.VectorGeometry geometry = VectorTile.encodeGeometry(polygon(noisyCircle(66_000, 80, 20)));
    double oneGridUnit = 256d / VectorTile.extent();

    IllegalStateException error = assertThrows(IllegalStateException.class,
      () -> enforceAndSerialize(geometry, 60_000, oneGridUnit));

    assertTrue(error.getMessage().contains("maximum tolerance"));
  }

  private static int[] enforceAndSerialize(VectorTile.VectorGeometry geometry, int maxVertices,
    double maxTolerance) {
    VectorTile tile = new VectorTile();
    tile.addLayerFeatures("test", List.of(new VectorTile.Feature("test", 123, geometry, Map.of("name", "shape"))));
    tile.enforceRendererPolygonLimit(Z0, maxVertices, maxTolerance);
    byte[] serialized = tile.toProto().toByteArray();
    try {
      VectorTileProto.Tile proto = VectorTileProto.Tile.parseFrom(serialized);
      return proto.getLayers(0).getFeatures(0).getGeometryList().stream().mapToInt(Integer::intValue).toArray();
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private static RenderPathResult renderFinalGeometry(Polygon worldPolygon, int extent) {
    PlanetilerConfig config = PlanetilerConfig.from(Arguments.of(
      "maxzoom", "0",
      "render_maxzoom", "0",
      "tile_extent", Integer.toString(extent)
    ));
    var source = SimpleFeature.create(
      GeoUtils.worldToLatLonCoords(worldPolygon), HashMap.newHashMap(0), null, null, 123);
    var feature = new FeatureCollector.Factory(config, Stats.inMemory()).get(source)
      .polygon("test")
      .setZoomRange(0, 0)
      .setMinPixelSize(0)
      .transformScaledGeometry(GeometryPipeline.NOOP);
    List<RenderedFeature> rendered = new ArrayList<>();
    new FeatureRenderer(config, rendered::add, Stats.inMemory()).accept(feature);
    assertEquals(1, rendered.size());
    VectorTile.VectorGeometry geometry = rendered.getFirst().vectorTileFeature().geometry();
    return new RenderPathResult(geometry, VectorTile.rendererPolygonVertexCount(geometry.commands()));
  }

  private static Polygon polygon(Coordinate[] coordinates) {
    return JTS_FACTORY.createPolygon(coordinates);
  }

  private static Coordinate[] noisyCircle(int vertices, double radius, double noise) {
    return noisyCircle(vertices, radius, noise, 128, 128);
  }

  private static Coordinate[] noisyCircle(int vertices, double radius, double noise, double centerX,
    double centerY) {
    Coordinate[] coordinates = new Coordinate[vertices + 1];
    for (int i = 0; i < vertices; i++) {
      double angle = 2 * Math.PI * i / vertices;
      double adjustedRadius = radius + noise * Math.sin(i * 17.0);
      coordinates[i] = new Coordinate(
        centerX + adjustedRadius * Math.cos(angle),
        centerY + adjustedRadius * Math.sin(angle)
      );
    }
    coordinates[vertices] = coordinates[0].copy();
    return coordinates;
  }

  private static Coordinate[] sawtoothCoastline(int columns, int pointsPerColumn) {
    List<Coordinate> coordinates = new ArrayList<>(columns * pointsPerColumn + 4);
    for (int column = 0; column < columns; column++) {
      double x = 0.05 + 0.9 * column / (columns - 1d);
      for (int row = 0; row < pointsPerColumn; row++) {
        int orderedRow = (column & 1) == 0 ? row : pointsPerColumn - 1 - row;
        double y = 0.2 + 0.5 * orderedRow / (pointsPerColumn - 1d);
        coordinates.add(new Coordinate(x, y));
      }
    }
    coordinates.add(new Coordinate(0.95, 0.95));
    coordinates.add(new Coordinate(0.05, 0.95));
    coordinates.add(coordinates.getFirst().copy());
    return coordinates.toArray(Coordinate[]::new);
  }

  private static Polygon polygonWithHoles(int holes, boolean lastIsSmallTriangle) {
    LinearRing shell = JTS_FACTORY.createLinearRing(closedRing(
      new Coordinate(1, 1), new Coordinate(255, 1), new Coordinate(255, 255), new Coordinate(1, 255)));
    LinearRing[] holeRings = new LinearRing[holes];
    for (int i = 0; i < holes; i++) {
      double x = 3 + (i % 25) * 10;
      double y = 3 + (i / 25) * 10;
      Coordinate[] coordinates;
      if (lastIsSmallTriangle && i == holes - 1) {
        coordinates = closedRing(
          new Coordinate(x, y), new Coordinate(x + 0.25, y), new Coordinate(x, y + 0.25));
      } else {
        coordinates = closedRing(
          new Coordinate(x, y), new Coordinate(x, y + 2), new Coordinate(x + 2, y + 2),
          new Coordinate(x + 2, y));
      }
      holeRings[i] = JTS_FACTORY.createLinearRing(coordinates);
    }
    return JTS_FACTORY.createPolygon(shell, holeRings);
  }

  private static Polygon detailedPolygonWithHoles(int holes, int verticesPerHole) {
    LinearRing shell = JTS_FACTORY.createLinearRing(closedRing(
      new Coordinate(1, 1), new Coordinate(255, 1), new Coordinate(255, 255), new Coordinate(1, 255)));
    LinearRing[] holeRings = new LinearRing[holes];
    for (int i = 0; i < holes; i++) {
      double x = 5 + (i % 25) * 10;
      double y = 5 + (i / 25) * 10;
      Coordinate[] coordinates = noisyCircle(verticesPerHole, 2, 0.2, x, y);
      reverse(coordinates);
      holeRings[i] = JTS_FACTORY.createLinearRing(coordinates);
    }
    return JTS_FACTORY.createPolygon(shell, holeRings);
  }

  private static void reverse(Coordinate[] coordinates) {
    for (int i = 0, j = coordinates.length - 1; i < j; i++, j--) {
      Coordinate swap = coordinates[i];
      coordinates[i] = coordinates[j];
      coordinates[j] = swap;
    }
  }

  private static Coordinate[] closedRing(Coordinate... coordinates) {
    Coordinate[] result = Arrays.copyOf(coordinates, coordinates.length + 1);
    result[coordinates.length] = coordinates[0].copy();
    return result;
  }

  private static int[] encodeRawRings(Coordinate[]... rings) {
    List<Integer> commands = new ArrayList<>();
    int previousX = 0;
    int previousY = 0;
    for (Coordinate[] ring : rings) {
      commands.add(9); // MoveTo, length 1
      int x = (int) ring[0].x;
      int y = (int) ring[0].y;
      commands.add(VectorTile.zigZagEncode(x - previousX));
      commands.add(VectorTile.zigZagEncode(y - previousY));
      previousX = x;
      previousY = y;
      commands.add(((ring.length - 1) << 3) | 2); // LineTo
      for (int i = 1; i < ring.length; i++) {
        x = (int) ring[i].x;
        y = (int) ring[i].y;
        commands.add(VectorTile.zigZagEncode(x - previousX));
        commands.add(VectorTile.zigZagEncode(y - previousY));
        previousX = x;
        previousY = y;
      }
      commands.add(15); // ClosePath, length 1
    }
    return commands.stream().mapToInt(Integer::intValue).toArray();
  }

  private record RenderPathResult(VectorTile.VectorGeometry geometry, int originalCount) {}
}
