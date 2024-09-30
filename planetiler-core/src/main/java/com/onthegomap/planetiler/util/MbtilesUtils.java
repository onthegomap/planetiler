package com.onthegomap.planetiler.util;

import static com.onthegomap.planetiler.geo.GeoUtils.JTS_FACTORY;
import static com.onthegomap.planetiler.geo.GeoUtils.coordinateSequence;
import static com.onthegomap.planetiler.util.Gzip.gunzip;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.collect.ImmutableMap;
import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.VectorTile;
import com.onthegomap.planetiler.archive.ReadableTileArchive;
import com.onthegomap.planetiler.archive.Tile;
import com.onthegomap.planetiler.archive.TileArchiveMetadata;
import com.onthegomap.planetiler.archive.TileCompression;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.mbtiles.Mbtiles;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.stats.Stats;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;
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

public class MbtilesUtils {

  public static final AffineTransformation TRANSFORM_TO_TILE = AffineTransformation
    .scaleInstance(256d / 4096d, 256d / 4096d);

  public static final TileArchiveMetadata MAX_METADATA_DESERIALIZED =
    new TileArchiveMetadata("name", "description", "attribution", "version", "type", "format", new Envelope(0, 1, 2, 3),
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
      "format":"format",
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
    return getTileMap(db, TileCompression.GZIP);
  }

  public static Map<TileCoord, List<ComparableFeature>> getTileMap(ReadableTileArchive db,
    TileCompression tileCompression)
    throws IOException {
    Map<TileCoord, List<ComparableFeature>> tiles = new TreeMap<>();
    for (var tile : getTiles(db)) {
      var bytes = switch (tileCompression) {
        case GZIP -> gunzip(tile.bytes());
        case NONE -> tile.bytes();
        case UNKNOWN -> throw new IllegalArgumentException("cannot decompress \"UNKNOWN\"");
      };
      var decoded = VectorTile.decode(bytes).stream()
        .map(
          feature -> feature(decodeSilently(feature.geometry()), feature.layer(), feature.tags(), feature.id()))
        .toList();
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





  public static Geometry emptyGeometry() {
    return GeoUtils.JTS_FACTORY.createGeometryCollection();
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
      var input = MbtilesUtils.class.getResourceAsStream("/" + resource);
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


}
