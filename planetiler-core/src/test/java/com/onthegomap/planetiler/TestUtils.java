package com.onthegomap.planetiler;

import static com.onthegomap.planetiler.geo.GeoUtils.JTS_FACTORY;
import static com.onthegomap.planetiler.geo.GeoUtils.coordinateSequence;
import static com.onthegomap.planetiler.util.Gzip.gunzip;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.mbtiles.Mbtiles;
import com.onthegomap.planetiler.mbtiles.Verify;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.stats.Stats;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
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
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.locationtech.jts.algorithm.Orientation;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.Envelope;
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
import org.locationtech.jts.geom.Polygonal;
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

  public static Polygon newPolygon(List<Coordinate> outer) {
    return newPolygon(outer, List.of());
  }

  public static Polygon newPolygon(List<Coordinate> outer, List<List<Coordinate>> inner) {
    return GeoUtils.JTS_FACTORY.createPolygon(
      GeoUtils.JTS_FACTORY.createLinearRing(outer.toArray(new Coordinate[0])),
      inner.stream().map(i -> GeoUtils.JTS_FACTORY.createLinearRing(i.toArray(new Coordinate[0])))
        .toArray(LinearRing[]::new)
    );
  }

  public static LineString newLineString(double... coords) {
    return newLineString(newCoordinateList(coords));
  }

  public static LineString newLineString(List<Coordinate> coords) {
    return GeoUtils.JTS_FACTORY.createLineString(coords.toArray(new Coordinate[0]));
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

  public static Geometry round(Geometry input, double delta) {
    return new GeometryTransformer() {
      @Override
      protected CoordinateSequence transformCoordinates(
        CoordinateSequence coords, Geometry parent) {
        for (int i = 0; i < coords.size(); i++) {
          for (int j = 0; j < coords.getDimension(); j++) {
            coords.setOrdinate(i, j, Math.round(coords.getOrdinate(i, j) * delta) / delta);
          }
        }
        return coords;
      }
    }.transform(input.copy());
  }

  public static Geometry round(Geometry input) {
    return round(input, 1e5);
  }

  public static Map<TileCoord, List<ComparableFeature>> getTileMap(Mbtiles db) throws SQLException, IOException {
    Map<TileCoord, List<ComparableFeature>> tiles = new TreeMap<>();
    for (var tile : getAllTiles(db)) {
      var bytes = gunzip(tile.bytes());
      var decoded = VectorTile.decode(bytes).stream()
        .map(feature -> feature(decodeSilently(feature.geometry()), feature.attrs())).toList();
      tiles.put(tile.tile(), decoded);
    }
    return tiles;
  }

  public static Geometry decodeSilently(VectorTile.VectorGeometry geom) {
    try {
      return geom.decode();
    } catch (GeometryException e) {
      throw new RuntimeException(e);
    }
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

  public static int getTilesDataCount(Mbtiles db) throws SQLException {
    String tableToCountFrom = isCompactDb(db) ? "tiles_data" : "tiles";
    try (Statement statement = db.connection().createStatement()) {
      ResultSet rs = statement.executeQuery("select count(*) from %s".formatted(tableToCountFrom));
      rs.next();
      return rs.getInt(1);
    }
  }

  public static boolean isCompactDb(Mbtiles db) throws SQLException {
    try (Statement statement = db.connection().createStatement()) {
      ResultSet rs = statement.executeQuery("select count(*) from sqlite_master where type='view' and name='tiles'");
      rs.next();
      return rs.getInt(1) > 0;
    }
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

  private static void validateGeometryRecursive(Geometry g) {
    if (g instanceof GeometryCollection gs) {
      for (int i = 0; i < gs.getNumGeometries(); i++) {
        validateGeometry(gs.getGeometryN(i));
      }
    } else if (g instanceof Point point) {
      assertFalse(point.isEmpty(), () -> "empty: " + point);
    } else if (g instanceof LineString line) {
      assertTrue(line.getNumPoints() >= 2, () -> "too few points: " + line);
    } else if (g instanceof Polygon poly) {
      var outer = poly.getExteriorRing();
      assertTrue(Orientation.isCCW(outer.getCoordinateSequence()), () -> "outer not CCW: " + poly);
      assertTrue(outer.getNumPoints() >= 4, () -> "outer too few points: " + poly);
      assertTrue(outer.isClosed(), () -> "outer not closed: " + poly);
      for (int i = 0; i < poly.getNumInteriorRing(); i++) {
        int _i = i;
        var inner = poly.getInteriorRingN(i);
        assertFalse(Orientation.isCCW(inner.getCoordinateSequence()),
          () -> "inner " + _i + " not CW: " + poly);
        assertTrue(outer.getNumPoints() >= 4, () -> "inner " + _i + " too few points: " + poly);
        assertTrue(inner.isClosed(), () -> "inner " + _i + " not closed: " + poly);
      }
    } else {
      fail("Unrecognized geometry: " + g);
    }
  }

  public static void validateGeometry(Geometry g) {
    if (g instanceof Polygonal) {
      assertTrue(g.isSimple(), "JTS isSimple()");
    }
    validateGeometryRecursive(g);
  }

  public static Path pathToResource(String resource) {
    Path cwd = Path.of("").toAbsolutePath();
    Path pathFromRoot = Path.of("planetiler-core", "src", "test", "resources", resource);
    return cwd.resolveSibling(pathFromRoot);
  }

  public interface GeometryComparision {

    Geometry geom();

    default void validate() {
      validateGeometry(geom());
    }
  }

  public record NormGeometry(Geometry geom) implements GeometryComparision {

    @Override
    public boolean equals(Object o) {
      return o instanceof GeometryComparision that && geom.equalsNorm(that.geom());
    }

    @Override
    public String toString() {
      return "Norm{" + geom.norm() + '}';
    }

    @Override
    public int hashCode() {
      return 0;
    }
  }

  private record ExactGeometry(Geometry geom) implements GeometryComparision {

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

  public record TopoGeometry(Geometry geom) implements GeometryComparision {

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

  public record ComparableFeature(
    GeometryComparision geometry,
    Map<String, Object> attrs
  ) {}

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
    result.put("_sortkey", feature.getSortKey());
    result.put("_geom", new NormGeometry(geom));
    result.put("_labelgrid_limit", feature.getPointLabelGridLimitAtZoom(zoom));
    result.put("_labelgrid_size", feature.getPointLabelGridPixelSizeAtZoom(zoom));
    result.put("_minpixelsize", feature.getMinPixelSizeAtZoom(zoom));
    result.put("_type", geom instanceof Puntal ? "point" : geom instanceof Lineal ? "line" :
      geom instanceof Polygonal ? "polygon" : geom.getClass().getSimpleName());
    result.put("_numpointsattr", feature.getNumPointsAttr());
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

  public static void assertSameNormalizedFeatures(
    Map<TileCoord, Collection<Geometry>> expected,
    Map<TileCoord, Collection<Geometry>> actual
  ) {
    assertEquals(
      mapTileFeatures(expected, NormGeometry::new),
      mapTileFeatures(actual, NormGeometry::new)
    );
  }

  public static void assertSameNormalizedFeature(Geometry expected, Geometry actual, Geometry... otherActuals) {
    assertEquals(new NormGeometry(expected), new NormGeometry(actual), "arg 2 != arg 1");
    if (otherActuals != null && otherActuals.length > 0) {
      for (int i = 0; i < otherActuals.length; i++) {
        assertEquals(new NormGeometry(expected), new NormGeometry(otherActuals[i]),
          "arg " + (i + 3) + " != arg 1");
      }
    }
  }

  public static void assertPointOnSurface(Geometry surface, Geometry actual) {
    assertTrue(surface.covers(actual),
      actual + System.lineSeparator() + "is not inside" + System.lineSeparator() + surface);
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

  public static void assertTopologicallyEquivalentFeature(Geometry expected, Geometry actual) {
    assertEquals(new TopoGeometry(expected), new TopoGeometry(actual));
  }

  public static List<Coordinate> worldCoordinateList(double... coords) {
    List<Coordinate> points = newCoordinateList(coords);
    points.forEach(c -> {
      c.x = GeoUtils.getWorldLon(c.x);
      c.y = GeoUtils.getWorldLat(c.y);
    });
    return points;
  }

  public static List<Coordinate> worldRectangle(double min, double max) {
    return worldCoordinateList(
      min, min,
      max, min,
      max, max,
      min, max,
      min, min
    );
  }

  public static void assertListsContainSameElements(List<?> expected, List<?> actual) {
    var comparator = Comparator.comparing(Object::toString);
    assertEquals(
      expected.stream().sorted(comparator).toList(),
      actual.stream().sorted(comparator).toList()
    );
  }

  public static LinearRing newLinearRing(double... coords) {
    return JTS_FACTORY.createLinearRing(coordinateSequence(coords));
  }

  @JacksonXmlRootElement(localName = "node")
  public record Node(
    long id, double lat, double lon
  ) {}

  @JacksonXmlRootElement(localName = "nd")
  public record NodeRef(
    long ref
  ) {}

  public record Tag(String k, String v) {}

  public record Way(
    long id,
    @JacksonXmlProperty(localName = "nd")
    @JacksonXmlElementWrapper(useWrapping = false) List<NodeRef> nodeRefs,
    @JacksonXmlProperty(localName = "tag")
    @JacksonXmlElementWrapper(useWrapping = false) List<Tag> tags
  ) {}

  @JacksonXmlRootElement(localName = "member")
  public record RelationMember(
    String type, long ref, String role
  ) {}

  @JacksonXmlRootElement(localName = "relation")
  public record Relation(
    long id,
    @JacksonXmlProperty(localName = "member")
    @JacksonXmlElementWrapper(useWrapping = false) List<RelationMember> members,
    @JacksonXmlProperty(localName = "tag")
    @JacksonXmlElementWrapper(useWrapping = false) List<Tag> tags
  ) {}

  //  @JsonIgnoreProperties(ignoreUnknown = true)
  public record OsmXml(
    String version,
    String generator,
    String copyright,
    String attribution,
    String license,
    @JacksonXmlProperty(localName = "node")
    @JacksonXmlElementWrapper(useWrapping = false) List<Node> nodes,
    @JacksonXmlProperty(localName = "way")
    @JacksonXmlElementWrapper(useWrapping = false) List<Way> ways,
    @JacksonXmlProperty(localName = "relation")
    @JacksonXmlElementWrapper(useWrapping = false) List<Relation> relation
  ) {}

  private static final XmlMapper xmlMapper = new XmlMapper();

  static {
    xmlMapper.registerModule(new Jdk8Module());
  }

  public static OsmXml readOsmXml(String s) throws IOException {
    Path path = pathToResource(s);
    return xmlMapper.readValue(Files.newInputStream(path), OsmXml.class);
  }

  public static FeatureCollector newFeatureCollectorFor(SourceFeature feature) {
    var featureCollectorFactory = new FeatureCollector.Factory(
      PlanetilerConfig.defaults(),
      Stats.inMemory()
    );
    return featureCollectorFactory.get(feature);
  }

  public static List<FeatureCollector.Feature> processSourceFeature(SourceFeature sourceFeature, Profile profile) {
    FeatureCollector collector = newFeatureCollectorFor(sourceFeature);
    profile.processFeature(sourceFeature, collector);
    List<FeatureCollector.Feature> result = new ArrayList<>();
    collector.forEach(result::add);
    return result;
  }


  public static void assertContains(String substring, String string) {
    if (!string.contains(substring)) {
      fail("'%s' did not contain '%s'".formatted(string, substring));
    }
  }

  public static void assertNumFeatures(Mbtiles db, String layer, int zoom, Map<String, Object> attrs,
    Envelope envelope, int expected, Class<? extends Geometry> clazz) {
    try {
      int num = Verify.getNumFeatures(db, layer, zoom, attrs, envelope, clazz);

      assertEquals(expected, num, "z%d features in %s".formatted(zoom, layer));
    } catch (GeometryException e) {
      fail(e);
    }
  }

  public static void assertMinFeatureCount(Mbtiles db, String layer, int zoom, Map<String, Object> attrs,
    Envelope envelope, int expected, Class<? extends Geometry> clazz) {
    try {
      int num = Verify.getNumFeatures(db, layer, zoom, attrs, envelope, clazz);

      assertTrue(expected < num,
        "z%d features in %s, expected at least %d got %d".formatted(zoom, layer, expected, num));
    } catch (GeometryException e) {
      fail(e);
    }
  }

  public static void assertFeatureNear(Mbtiles db, String layer, Map<String, Object> attrs, double lng, double lat,
    int minzoom, int maxzoom) {
    try {
      List<String> failures = new ArrayList<>();
      outer: for (int zoom = 0; zoom <= 14; zoom++) {
        boolean shouldFind = zoom >= minzoom && zoom <= maxzoom;
        var coord = TileCoord.aroundLngLat(lng, lat, zoom);
        Geometry tilePoint = GeoUtils.point(coord.lngLatToTileCoords(lng, lat));
        byte[] tile = db.getTile(coord);
        List<VectorTile.Feature> features = tile == null ? List.of() : VectorTile.decode(gunzip(tile));

        Set<String> containedInLayers = new TreeSet<>();
        Set<String> containedInLayerFeatures = new TreeSet<>();
        for (var feature : features) {
          if (feature.geometry().decode().isWithinDistance(tilePoint, 2)) {
            containedInLayers.add(feature.layer());
            if (layer.equals(feature.layer())) {
              Map<String, Object> tags = feature.attrs();
              containedInLayerFeatures.add(tags.toString());
              if (tags.entrySet().containsAll(attrs.entrySet())) {
                // found a match
                if (!shouldFind) {
                  failures.add("z%d found feature but should not have".formatted(zoom));
                }
                continue outer;
              }
            }
          }
        }

        // not found
        if (shouldFind) {
          if (containedInLayers.isEmpty()) {
            failures.add("z%d no features were found in any layer".formatted(zoom));
          } else if (!containedInLayers.contains(layer)) {
            failures.add("z%d features found in %s but not %s".formatted(
              zoom, containedInLayers, layer
            ));
          } else {
            failures.add("z%d features found in %s but had wrong tags: %s".formatted(
              zoom, layer, containedInLayerFeatures.stream()
                .collect(Collectors.joining(System.lineSeparator(), System.lineSeparator(), "")))
            );
          }
        }
      }
      if (!failures.isEmpty()) {
        fail(String.join(System.lineSeparator(), failures));
      }
    } catch (GeometryException | IOException e) {
      fail(e);
    }
  }
}
