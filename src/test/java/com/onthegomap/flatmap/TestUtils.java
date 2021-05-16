package com.onthegomap.flatmap;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
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
    return GeoUtils.gf.createPolygon(newCoordinateList(coords).toArray(new Coordinate[0]));
  }

  public static LineString newLineString(double... coords) {
    return GeoUtils.gf.createLineString(newCoordinateList(coords).toArray(new Coordinate[0]));
  }

  public static Point newPoint(double x, double y) {
    return GeoUtils.gf.createPoint(new CoordinateXY(x, y));
  }

  public static MultiPoint newMultiPoint(Point... points) {
    return GeoUtils.gf.createMultiPoint(points);
  }

  public static MultiPolygon newMultiPolygon(Polygon... polys) {
    return GeoUtils.gf.createMultiPolygon(polys);
  }

  public static GeometryCollection newGeometryCollection(Geometry... geoms) {
    return GeoUtils.gf.createGeometryCollection(geoms);
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
    return GeoUtils.gf.createGeometryCollection();
  }

  public interface GeometryComparision {

    Geometry geom();
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
    return new ComparableFeature(new TopoGeometry(geom), attrs);
  }

  public static Map<String, Object> toMap(FeatureCollector.Feature<?> feature, int zoom) {
    TreeMap<String, Object> result = new TreeMap<>(feature.getAttrsAtZoom(zoom));
    result.put("_minzoom", feature.getMinZoom());
    result.put("_maxzoom", feature.getMaxZoom());
    result.put("_buffer", feature.getBufferPixelsAtZoom(zoom));
    result.put("_layer", feature.getLayer());
    result.put("_zorder", feature.getZorder());
    result.put("_geom", new TopoGeometry(feature.getGeometry()));
    if (feature instanceof FeatureCollector.PointFeature pointFeature) {
      result.put("_type", "point");
      result.put("_labelgrid_limit", pointFeature.getLabelGridLimitAtZoom(zoom));
      result.put("_labelgrid_size", pointFeature.getLabelGridPixelSizeAtZoom(zoom));
    } else if (feature instanceof FeatureCollector.LineFeature lineFeature) {
      result.put("_type", "line");
      result.put("_minlength", lineFeature.getMinLengthAtZoom(zoom));
    } else if (feature instanceof FeatureCollector.PolygonFeature polygonFeature) {
      result.put("_type", "polygon");
      result.put("_minarea", polygonFeature.getMinAreaAtZoom(zoom));
    }
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
