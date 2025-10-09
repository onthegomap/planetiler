package com.onthegomap.planetiler;

import static com.onthegomap.planetiler.geo.GeoUtils.JTS_FACTORY;
import static com.onthegomap.planetiler.geo.GeoUtils.coordinateSequence;
import static com.onthegomap.planetiler.util.Gzip.gunzip;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.collect.ImmutableMap;
import com.onthegomap.planetiler.archive.ReadableTileArchive;
import com.onthegomap.planetiler.archive.Tile;
import com.onthegomap.planetiler.archive.TileArchiveMetadata;
import com.onthegomap.planetiler.archive.TileCompression;
import com.onthegomap.planetiler.archive.TileFormat;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.mbtiles.Mbtiles;
import com.onthegomap.planetiler.mbtiles.Verify;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.util.LayerAttrStats;
import com.onthegomap.planetiler.validator.BaseSchemaValidator;
import com.onthegomap.planetiler.validator.SchemaSpecification;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.jupiter.api.DynamicNode;
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

  public static final TileArchiveMetadata MAX_METADATA_DESERIALIZED =
    new TileArchiveMetadata("name", "description", "attribution", "version", "type", TileFormat.MVT,
      new Envelope(0, 1, 2, 3),
      new Coordinate(1.3, 3.7, 1.0), 2, 3,
      TileArchiveMetadata.TileArchiveMetadataJson.create(
        List.of(
          new LayerAttrStats.VectorLayer("vl0",
            ImmutableMap.of("1", LayerAttrStats.FieldType.BOOLEAN, "2", LayerAttrStats.FieldType.NUMBER, "3",
              LayerAttrStats.FieldType.STRING),
            Optional.of("description"), OptionalInt.of(1), OptionalInt.of(2)),
          new LayerAttrStats.VectorLayer("vl1",
            Map.of(),
            Optional.empty(), OptionalInt.empty(), OptionalInt.empty())
        )
      ),
      ImmutableMap.of("a", "b", "c", "d"),
      TileCompression.GZIP);
  public static final String MAX_METADATA_SERIALIZED = """
    {
      "name":"name",
      "description":"description",
      "attribution":"attribution",
      "version":"version",
      "type":"type",
      "format":"pbf",
      "minzoom":"2",
      "maxzoom":"3",
      "compression":"gzip",
      "bounds":"0,2,1,3",
      "center":"1.3,3.7,1",
      "json": "{
        \\"vector_layers\\":[
          {
            \\"id\\":\\"vl0\\",
            \\"fields\\":{
              \\"1\\":\\"Boolean\\",
              \\"2\\":\\"Number\\",
              \\"3\\":\\"String\\"
            },
            \\"description\\":\\"description\\",
            \\"minzoom\\":1,
            \\"maxzoom\\":2
          },
          {
            \\"id\\":\\"vl1\\",
            \\"fields\\":{}
          }
        ]
      }",
      "a":"b",
      "c":"d"
    }""".lines().map(String::trim).collect(Collectors.joining(""));

  public static final TileArchiveMetadata MIN_METADATA_DESERIALIZED =
    new TileArchiveMetadata(null, null, null, null, null, null, null, null, null, null, null, null, null);
  public static final String MIN_METADATA_SERIALIZED = "{}";

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

  public static Map<TileCoord, List<ComparableFeature>> getTileMap(ReadableTileArchive db)
    throws IOException {
    return getTileMap(db, TileCompression.GZIP, TileFormat.MVT);
  }

  public static Map<TileCoord, List<ComparableFeature>> getTileMap(ReadableTileArchive db,
    TileCompression tileCompression, TileFormat tileFormat)
    throws IOException {
    Map<TileCoord, List<ComparableFeature>> tiles = new TreeMap<>();
    for (var tile : getTiles(db)) {
      var bytes = switch (tileCompression) {
        case GZIP -> gunzip(tile.bytes());
        case NONE -> tile.bytes();
        case UNKNOWN -> throw new IllegalArgumentException("cannot decompress \"UNKNOWN\"");
      };
      var decoded = switch (tileFormat) {
        case MLT -> throw new UnsupportedOperationException("TODO decode MLTs");
        case UNKNOWN, MVT -> VectorTile.decode(bytes).stream()
          .map(
            feature -> feature(decodeSilently(feature.geometry()), feature.layer(), feature.tags(), feature.id()))
          .toList();
      };
      tiles.put(tile.coord(), decoded);
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

  public static Set<Tile> getTiles(ReadableTileArchive db) {
    return db.getAllTiles().stream().collect(Collectors.toSet());
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

  public static Path extractPathToResource(Path tempDir, String resource) {
    return extractPathToResource(tempDir, resource, resource);
  }

  public static Path extractPathToResource(Path tempDir, String resource, String local) {
    var path = tempDir.resolve(resource);
    try (
      var input = TestUtils.class.getResourceAsStream("/" + resource);
      var output = Files.newOutputStream(path);
    ) {
      Objects.requireNonNull(input, "Could not find " + resource + " on classpath").transferTo(output);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return path;
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

  public record RoundGeometry(Geometry geom) implements GeometryComparision {

    @Override
    public boolean equals(Object o) {
      return o instanceof GeometryComparision that && round(geom).equalsNorm(round(that.geom()));
    }

    @Override
    public String toString() {
      return "Round{" + round(geom).norm() + '}';
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
    String layer,
    Map<String, Object> attrs,
    Long id
  ) {
    ComparableFeature(
      GeometryComparision geometry,
      String layer,
      Map<String, Object> attrs
    ) {
      this(geometry, layer, attrs, null);
    }

    @Override
    public boolean equals(Object o) {
      return o == this || (o instanceof ComparableFeature other &&
        (id == null || other.id == null || id.equals(other.id)) &&
        geometry.equals(other.geometry) &&
        attrs.equals(other.attrs) &&
        (layer == null || other.layer == null || Objects.equals(layer, other.layer)));
    }

    @Override
    public int hashCode() {
      int result = geometry.hashCode();
      result = 31 * result + attrs.hashCode();
      return result;
    }

    ComparableFeature withId(long id) {
      return new ComparableFeature(geometry, layer, attrs, id);
    }
  }


  public static ComparableFeature feature(Geometry geom, String layer, Map<String, Object> attrs, long id) {
    return new ComparableFeature(new NormGeometry(geom), layer, attrs, id);
  }

  public static ComparableFeature feature(Geometry geom, String layer, Map<String, Object> attrs) {
    return new ComparableFeature(new NormGeometry(geom), layer, attrs);
  }

  public static ComparableFeature feature(Geometry geom, Map<String, Object> attrs, long id) {
    return new ComparableFeature(new NormGeometry(geom), null, attrs, id);
  }

  public static ComparableFeature feature(Geometry geom, Map<String, Object> attrs) {
    return new ComparableFeature(new NormGeometry(geom), null, attrs);
  }

  public static Map<String, Object> toMap(FeatureCollector.Feature feature, int zoom) {
    TreeMap<String, Object> result = new TreeMap<>(feature.getAttrsAtZoom(zoom));
    Geometry geom = feature.getGeometry();
    result.put("_id", feature.getId());
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
    long id, double lat, double lon,
    @JacksonXmlProperty(localName = "tag")
    @JacksonXmlElementWrapper(useWrapping = false) List<Tag> tags
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
              Map<String, Object> tags = feature.tags();
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

  public static void assertTileDuplicates(Mbtiles db, int expected) {
    try {
      Connection connection = (Connection) FieldUtils.readField(db, "connection", true);
      Statement statement = connection.createStatement();
      ResultSet rs = statement.executeQuery("SELECT tile_data FROM tiles_data");
      ArrayList<byte[]> tilesList = new ArrayList<>();
      while (rs.next()) {
        tilesList.add(rs.getBytes("tile_data"));
      }

      var tiles = tilesList.toArray(new byte[0][0]);
      Set<Integer> dups = new HashSet<>();
      for (int i = 0; i < tiles.length; i++) {
        for (int j = i + 1; j < tiles.length; j++) {
          if (Arrays.equals(tiles[i], tiles[j])) {
            if (!dups.contains(j)) {
              dups.add(j);
            }
          }
        }
      }

      int dupCount = dups.size();
      assertEquals(expected, dupCount, "%d duplicates expected, %d found".formatted(expected, dupCount));
    } catch (IllegalAccessException | SQLException e) {
      fail(e);
    }
  }

  public static Stream<DynamicNode> validateProfile(Profile profile, String spec) {
    return validateProfile(profile, SchemaSpecification.load(spec));
  }

  public static Stream<DynamicNode> validateProfile(Profile profile, SchemaSpecification spec) {
    var result = BaseSchemaValidator.validate(profile, spec, PlanetilerConfig.defaults());
    return result.results().stream().map(test -> dynamicTest(test.example().name(), () -> {
      var issues = test.issues().get();
      if (!issues.isEmpty()) {
        fail("Failed with " + issues.size() + " issues:\n" + String.join("\n", issues));
      }
    }));
  }
}
