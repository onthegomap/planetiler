package com.onthegomap.flatmap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.onthegomap.flatmap.geo.GeoUtils;
import com.onthegomap.flatmap.geo.TileCoord;
import com.onthegomap.flatmap.write.Mbtiles;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import org.locationtech.jts.algorithm.Orientation;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Lineal;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.Puntal;
import org.locationtech.jts.geom.util.AffineTransformation;
import org.locationtech.jts.geom.util.GeometryTransformer;

public class TestUtils {

  public static final AffineTransformation TRANSFORM_TO_TILE = AffineTransformation
    .scaleInstance(256d / 4096d, 256d / 4096d);

  public static List<Coordinate> newCoordinateList(double... coords) {
    List<Coordinate> result = new ArrayList<>(coords.length / 2);
    for (int i = 0; i < coords.length; i += 2) {
      result.add(new CoordinateXY(coords[i], coords[i + 1]));
    }
    return result;
  }

  public static Polygon newPolygon(double... coords) {
    return GeoUtils.JTS_FACTORY.createPolygon(newCoordinateList(coords).toArray(new Coordinate[0]));
  }

  public static Polygon tileBottomRight(double buffer) {
    return rectangle(128, 128, 256 + buffer, 256 + buffer);
  }

  public static Polygon tileBottom(double buffer) {
    return rectangle(-buffer, 128, 256 + buffer, 256 + buffer);
  }

  public static Polygon tileBottomLeft(double buffer) {
    return rectangle(-buffer, 128, 128, 256 + buffer);
  }

  public static Polygon tileLeft(double buffer) {
    return rectangle(-buffer, -buffer, 128, 256 + buffer);
  }

  public static Polygon tileTopLeft(double buffer) {
    return rectangle(-buffer, -buffer, 128, 128);
  }

  public static Polygon tileTop(double buffer) {
    return rectangle(-buffer, -buffer, 256 + buffer, 128);
  }

  public static Polygon tileTopRight(double buffer) {
    return rectangle(128, -buffer, 256 + buffer, 128);
  }

  public static Polygon tileRight(double buffer) {
    return rectangle(128, -buffer, 256 + buffer, 256 + buffer);
  }

  public static List<Coordinate> tileFill(double buffer) {
    return rectangleCoordList(-buffer, 256 + buffer);
  }

  public static List<Coordinate> rectangleCoordList(double minX, double minY, double maxX, double maxY) {
    return newCoordinateList(
      minX, minY,
      maxX, minY,
      maxX, maxY,
      minX, maxY,
      minX, minY
    );
  }

  public static List<Coordinate> rectangleCoordList(double min, double max) {
    return rectangleCoordList(min, min, max, max);
  }

  public static Polygon rectangle(double minX, double minY, double maxX, double maxY) {
    return newPolygon(rectangleCoordList(minX, minY, maxX, maxY), List.of());
  }

  public static Polygon rectangle(double min, double max) {
    return rectangle(min, min, max, max);
  }

  public static Polygon newPolygon(List<Coordinate> outer, List<List<Coordinate>> inner) {
    return GeoUtils.JTS_FACTORY.createPolygon(
      GeoUtils.JTS_FACTORY.createLinearRing(outer.toArray(new Coordinate[0])),
      inner.stream().map(i -> GeoUtils.JTS_FACTORY.createLinearRing(i.toArray(new Coordinate[0])))
        .toArray(LinearRing[]::new)
    );
  }

  public static LineString newLineString(double... coords) {
    return GeoUtils.JTS_FACTORY.createLineString(newCoordinateList(coords).toArray(new Coordinate[0]));
  }

  public static MultiLineString newMultiLineString(LineString... lineStrings) {
    return GeoUtils.JTS_FACTORY.createMultiLineString(lineStrings);
  }

  public static Point newPoint(double x, double y) {
    return GeoUtils.JTS_FACTORY.createPoint(new CoordinateXY(x, y));
  }

  public static MultiPoint newMultiPoint(Point... points) {
    return GeoUtils.JTS_FACTORY.createMultiPoint(points);
  }

  public static MultiPolygon newMultiPolygon(Polygon... polys) {
    return GeoUtils.JTS_FACTORY.createMultiPolygon(polys);
  }

  public static GeometryCollection newGeometryCollection(Geometry... geoms) {
    return GeoUtils.JTS_FACTORY.createGeometryCollection(geoms);
  }

  public static Geometry round(Geometry input) {
    return new GeometryTransformer() {
      @Override
      protected CoordinateSequence transformCoordinates(
        CoordinateSequence coords, Geometry parent) {
        for (int i = 0; i < coords.size(); i++) {
          for (int j = 0; j < coords.getDimension(); j++) {
            coords.setOrdinate(i, j, Math.round(coords.getOrdinate(i, j) * 1e5) / 1e5);
          }
        }
        return coords;
      }
    }.transform(input);
  }

  private static byte[] gunzip(byte[] zipped) throws IOException {
    try (var is = new GZIPInputStream(new ByteArrayInputStream(zipped))) {
      return is.readAllBytes();
    }
  }

  public static Map<TileCoord, List<ComparableFeature>> getTileMap(Mbtiles db) throws SQLException, IOException {
    Map<TileCoord, List<ComparableFeature>> tiles = new TreeMap<>();
    for (var tile : getAllTiles(db)) {
      var bytes = gunzip(tile.bytes());
      var decoded = VectorTileEncoder.decode(bytes).stream()
        .map(feature -> feature(feature.geometry().decode(), feature.attrs())).toList();
      tiles.put(tile.tile(), decoded);
    }
    return tiles;
  }

  public static Set<Mbtiles.TileEntry> getAllTiles(Mbtiles db) throws SQLException {
    Set<Mbtiles.TileEntry> result = new HashSet<>();
    try (Statement statement = db.connection().createStatement()) {
      ResultSet rs = statement.executeQuery("select zoom_level, tile_column, tile_row, tile_data from tiles");
      while (rs.next()) {
        int z = rs.getInt("zoom_level");
        int rawy = rs.getInt("tile_row");
        int x = rs.getInt("tile_column");
        result.add(new Mbtiles.TileEntry(
          TileCoord.ofXYZ(x, (1 << z) - 1 - rawy, z),
          rs.getBytes("tile_data")
        ));
      }
    }
    return result;
  }

  public static <K extends Comparable<? super K>, V> void assertMapEquals(Map<K, V> expected, Map<K, V> actual) {
    assertEquals(new TreeMap<>(expected), actual);
  }

  public static <K extends Comparable<? super K>> void assertSubmap(Map<K, ?> expectedSubmap, Map<K, ?> actual) {
    assertSubmap(expectedSubmap, actual, "");
  }

  public static <K extends Comparable<? super K>> void assertSubmap(Map<K, ?> expectedSubmap, Map<K, ?> actual,
    String message) {
    Map<K, Object> actualFiltered = new TreeMap<>();
    Map<K, Object> others = new TreeMap<>();
    for (K key : actual.keySet()) {
      Object value = actual.get(key);
      if (expectedSubmap.containsKey(key)) {
        actualFiltered.put(key, value);
      } else {
        others.put(key, value);
      }
    }
    for (K key : expectedSubmap.keySet()) {
      if ("<null>".equals(expectedSubmap.get(key))) {
        if (!actual.containsKey(key)) {
          actualFiltered.put(key, "<null>");
        }
      }
    }
    assertEquals(new TreeMap<>(expectedSubmap), actualFiltered, message + " others: " + others);
  }

  public static Geometry emptyGeometry() {
    return GeoUtils.JTS_FACTORY.createGeometryCollection();
  }

  public static void validateGeometry(Geometry g) {
    assertTrue(g.isValid(), "JTS isValid()");
    if (g instanceof GeometryCollection gs) {
      for (int i = 0; i < gs.getNumGeometries(); i++) {
        validateGeometry(gs.getGeometryN(i));
      }
    } else if (g instanceof Point point) {
      assertFalse(point.isEmpty(), "empty: " + point);
    } else if (g instanceof LineString line) {
      assertTrue(line.getNumPoints() >= 2, "too few points: " + line);
    } else if (g instanceof Polygon poly) {
      var outer = poly.getExteriorRing();
      assertTrue(Orientation.isCCW(outer.getCoordinateSequence()), "outer not CCW: " + poly);
      assertTrue(outer.getNumPoints() >= 4, "outer too few points: " + poly);
      assertTrue(outer.isClosed(), "outer not closed: " + poly);
      for (int i = 0; i < poly.getNumInteriorRing(); i++) {
        var inner = poly.getInteriorRingN(i);
        assertFalse(Orientation.isCCW(inner.getCoordinateSequence()),
          "inner " + i + " not CW: " + poly);
        assertTrue(outer.getNumPoints() >= 4, "inner " + i + " too few points: " + poly);
        assertTrue(inner.isClosed(), "inner " + i + " not closed: " + poly);
      }
    } else {
      fail("Unrecognized geometry: " + g);
    }
  }

  public interface GeometryComparision {

    Geometry geom();

    default void validate() {
      validateGeometry(geom());
    }
  }

  private static record NormGeometry(Geometry geom) implements GeometryComparision {

    @Override
    public boolean equals(Object o) {
      return o instanceof GeometryComparision that && geom.equalsNorm(that.geom());
    }

    @Override
    public String toString() {
      return "Norm{" + geom + '}';
    }

    @Override
    public int hashCode() {
      return 0;
    }
  }

  private static record ExactGeometry(Geometry geom) implements GeometryComparision {

    @Override
    public boolean equals(Object o) {
      return o instanceof GeometryComparision that && geom.equalsExact(that.geom());
    }

    @Override
    public String toString() {
      return "Exact{" + geom + '}';
    }

    @Override
    public int hashCode() {
      return 0;
    }
  }

  private static record TopoGeometry(Geometry geom) implements GeometryComparision {

    @Override
    public boolean equals(Object o) {
      return o instanceof GeometryComparision that && geom.equalsTopo(that.geom());
    }

    @Override
    public String toString() {
      return "Topo{" + geom + '}';
    }

    @Override
    public int hashCode() {
      return 0;
    }
  }

  public static record ComparableFeature(
    GeometryComparision geometry,
    Map<String, Object> attrs
  ) {

  }

  public static ComparableFeature feature(Geometry geom, Map<String, Object> attrs) {
    return new ComparableFeature(new NormGeometry(geom), attrs);
  }

  public static Map<String, Object> toMap(FeatureCollector.Feature feature, int zoom) {
    TreeMap<String, Object> result = new TreeMap<>(feature.getAttrsAtZoom(zoom));
    Geometry geom = feature.getGeometry();
    result.put("_minzoom", feature.getMinZoom());
    result.put("_maxzoom", feature.getMaxZoom());
    result.put("_buffer", feature.getBufferPixelsAtZoom(zoom));
    result.put("_layer", feature.getLayer());
    result.put("_zorder", feature.getZorder());
    result.put("_geom", new NormGeometry(geom));
    result.put("_labelgrid_limit", feature.getLabelGridLimitAtZoom(zoom));
    result.put("_labelgrid_size", feature.getLabelGridPixelSizeAtZoom(zoom));
    result.put("_minpixelsize", feature.getMinPixelSize(zoom));
    result.put("_type", geom instanceof Puntal ? "point" : geom instanceof Lineal ? "line" : "polygon");
    return result;
  }

  private static final ObjectMapper objectMapper = new ObjectMapper();

  public static void assertSameJson(String expected, String actual) throws JsonProcessingException {
    assertEquals(
      objectMapper.readTree(expected),
      objectMapper.readTree(actual)
    );
  }

  public static <T> Map<TileCoord, Collection<T>> mapTileFeatures(Map<TileCoord, Collection<Geometry>> in,
    Function<Geometry, T> fn) {
    TreeMap<TileCoord, Collection<T>> out = new TreeMap<>();
    for (var entry : in.entrySet()) {
      out.put(entry.getKey(), entry.getValue().stream().map(fn)
        .sorted(Comparator.comparing(Object::toString))
        .collect(Collectors.toList()));
    }
    return out;
  }

  public static void assertExactSameFeatures(
    Map<TileCoord, Collection<Geometry>> expected,
    Map<TileCoord, Collection<Geometry>> actual
  ) {
    assertEquals(
      mapTileFeatures(expected, ExactGeometry::new),
      mapTileFeatures(actual, ExactGeometry::new)
    );
  }

  public static void assertTopologicallyEquivalentFeatures(
    Map<TileCoord, Collection<Geometry>> expected,
    Map<TileCoord, Collection<Geometry>> actual
  ) {
    assertEquals(
      mapTileFeatures(expected, TopoGeometry::new),
      mapTileFeatures(actual, TopoGeometry::new)
    );
  }

  public static void assertSameNormalizedFeatures(
    Map<TileCoord, Collection<Geometry>> expected,
    Map<TileCoord, Collection<Geometry>> actual
  ) {
    assertEquals(
      mapTileFeatures(expected, NormGeometry::new),
      mapTileFeatures(actual, NormGeometry::new)
    );
  }
}
