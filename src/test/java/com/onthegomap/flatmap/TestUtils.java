package com.onthegomap.flatmap;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.onthegomap.flatmap.geo.GeoUtils;
import com.onthegomap.flatmap.geo.TileCoord;
import com.onthegomap.flatmap.write.Mbtiles;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
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

  public static Map<TileCoord, List<ComparableFeature>> getTileMap(Mbtiles db) throws SQLException {
    Map<TileCoord, List<ComparableFeature>> tiles = new TreeMap<>();
    for (var tile : getAllTiles(db)) {
      var decoded = VectorTileEncoder.decode(tile.bytes()).stream()
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
        result.add(new Mbtiles.TileEntry(
          TileCoord.ofXYZ(
            rs.getInt("tile_column"),
            rs.getInt("tile_row"),
            rs.getInt("zoom_level")
          ),
          rs.getBytes("tile_data")
        ));
      }
    }
    return result;
  }

  public static <K extends Comparable<? super K>, V> void assertMapEquals(Map<K, V> expected, Map<K, V> actual) {
    assertEquals(new TreeMap<>(expected), actual);
  }

  public static <K extends Comparable<? super K>, V> void assertSubmap(Map<K, V> expectedSubmap, Map<K, V> actual) {
    Map<K, V> actualFiltered = new TreeMap<>();
    Map<K, V> others = new TreeMap<>();
    for (K key : actual.keySet()) {
      V value = actual.get(key);
      if (expectedSubmap.containsKey(key)) {
        actualFiltered.put(key, value);
      } else {
        others.put(key, value);
      }
    }
    assertEquals(new TreeMap<>(expectedSubmap), actualFiltered, "others: " + others);
  }

  public interface GeometryComparision {

    Geometry geom();
  }

  public static record ExactGeometry(Geometry geom) implements GeometryComparision {

    @Override
    public boolean equals(Object o) {
      return o instanceof GeometryComparision that && geom.equalsExact(that.geom());
    }
  }

  public static record TopoGeometry(Geometry geom) implements GeometryComparision {

    @Override
    public boolean equals(Object o) {
      return o instanceof GeometryComparision that && geom.equalsTopo(that.geom());
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
}
