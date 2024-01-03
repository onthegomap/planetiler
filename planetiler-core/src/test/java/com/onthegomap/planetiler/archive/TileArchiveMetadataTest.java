package com.onthegomap.planetiler.archive;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.TestUtils;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.util.LayerAttrStats;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.Envelope;

class TileArchiveMetadataTest {

  // produced by tilelive-copy or rather mbutil - just reduced $.json.vector_layers[]
  private static final String SERIALIZED =
    """
      {
          "attribution": "<a href=\\"https://www.openmaptiles.org/\\" target=\\"_blank\\">&copy; OpenMapTiles</a> <a href=\\"https://www.openstreetmap.org/copyright\\" target=\\"_blank\\">&copy; OpenStreetMap contributors</a>",\s
          "description": "A tileset showcasing all layers in OpenMapTiles. https://openmaptiles.org",\s
          "format": "pbf",\s
          "planetiler:version": "0.7-SNAPSHOT",\s
          "bounds": "7.40921,43.72335,7.44864,43.75169",\s
          "name": "OpenMapTiles",\s
          "planetiler:githash": "09c22c18268d9cc1371ed0b0af192e698abf54c7",\s
          "json": "{\\"vector_layers\\":[{\\"fields\\":{\\"class\\":\\"String\\",\\"ref\\":\\"String\\"},\\"id\\":\\"aeroway\\",\\"maxzoom\\":14,\\"minzoom\\":12}]}",\s
          "version": "3.14.0",\s
          "compression": "gzip",\s
          "minzoom": "0",\s
          "planetiler:osm:osmosisreplicationurl": "http://download.geofabrik.de/europe/monaco-updates",\s
          "maxzoom": "14",\s
          "planetiler:osm:osmosisreplicationseq": "3911",\s
          "type": "baselayer",\s
          "planetiler:buildtime": "2023-12-20T21:33:49.594Z",\s
          "planetiler:osm:osmosisreplicationtime": "2023-12-18T21:21:01Z",\s
          "center": "7.42892,43.73752,14"
      }
      """;

  private static final TileArchiveMetadata DESERIALIZED = new TileArchiveMetadata(
    "OpenMapTiles",
    "A tileset showcasing all layers in OpenMapTiles. https://openmaptiles.org",
    "<a href=\"https://www.openmaptiles.org/\" target=\"_blank\">&copy; OpenMapTiles</a> <a href=\"https://www.openstreetmap.org/copyright\" target=\"_blank\">&copy; OpenStreetMap contributors</a>",
    "3.14.0",
    "baselayer",
    "pbf",
    new Envelope(7.40921, 7.44864, 43.72335, 43.75169),
    new Coordinate(7.42892, 43.73752, 14),
    0,
    14,
    new TileArchiveMetadata.TileArchiveMetadataJson(
      List.of(
        new LayerAttrStats.VectorLayer(
          "aeroway",
          Map.of(
            "ref", LayerAttrStats.FieldType.STRING,
            "class", LayerAttrStats.FieldType.STRING
          ),
          12,
          14
        )
      )
    ),
    Map.of(
      "planetiler:version", "0.7-SNAPSHOT",
      "planetiler:githash", "09c22c18268d9cc1371ed0b0af192e698abf54c7",
      "planetiler:osm:osmosisreplicationurl", "http://download.geofabrik.de/europe/monaco-updates",
      "planetiler:osm:osmosisreplicationseq", "3911",
      "planetiler:buildtime", "2023-12-20T21:33:49.594Z",
      "planetiler:osm:osmosisreplicationtime", "2023-12-18T21:21:01Z"
    ),
    TileCompression.GZIP
  );

  private final JsonMapper jsonMapper = TileArchiveMetadataDeSer.mbtilesMapper();

  private final JsonMapper jsonMapperStrict = TileArchiveMetadataDeSer.newBaseBuilder()
    .addMixIn(TileArchiveMetadata.class, TileArchiveMetadataDeSer.StrictDeserializationMixin.class)
    .build();

  @Test
  void testDeserialization() throws JsonProcessingException {
    var actualDeserialized = jsonMapper.readValue(SERIALIZED, TileArchiveMetadata.class);
    assertEquals(DESERIALIZED, actualDeserialized);
  }

  @Test
  void testSerialization() throws JsonProcessingException {

    final ObjectNode o0 = (ObjectNode) jsonMapper.readTree(SERIALIZED);
    final ObjectNode o1 = (ObjectNode) jsonMapper.readTree(jsonMapper.writeValueAsString(DESERIALIZED));

    // string-escaped JSON might change order => parse JSON
    TestUtils.assertSameJson(
      o0.get("json").asText(),
      o1.get("json").asText()
    );
    o0.remove("json");
    o1.remove("json");

    assertEquals(o0, o1);
  }

  @Test
  void testCenterDeserialization() throws JsonProcessingException {

    final String s0 = """
      {"center": null}
      """;
    assertNull(jsonMapper.readValue(s0, TileArchiveMetadata.class).center());

    final String s1 = """
      {"center": "0.0,1.1"}
      """;
    assertEqualsCoordinate(new CoordinateXY(0.0, 1.1), jsonMapper.readValue(s1, TileArchiveMetadata.class).center());

    final String s2 = """
      {"center": "0.0,1.1,14"}
      """;
    assertEqualsCoordinate(new Coordinate(0.0, 1.1, 14), jsonMapper.readValue(s2, TileArchiveMetadata.class).center());

    final String s3 = """
      {"center": "0.0,1.1,14,42"}
      """;
    assertEqualsCoordinate(new Coordinate(0.0, 1.1, 14), jsonMapper.readValue(s3, TileArchiveMetadata.class).center());
    assertThrows(JsonMappingException.class, () -> jsonMapperStrict.readValue(s3, TileArchiveMetadata.class));

    final String s4 = """
      {"center": "0.0"}
      """;
    assertNull(jsonMapper.readValue(s4, TileArchiveMetadata.class).center());
    assertThrows(JsonMappingException.class, () -> jsonMapperStrict.readValue(s4, TileArchiveMetadata.class));
  }

  @Test
  void testBoundsDeserialization() throws JsonProcessingException {

    final String s0 = """
      {"bounds": null}
      """;
    assertNull(jsonMapper.readValue(s0, TileArchiveMetadata.class).bounds());

    final String s1 = """
      {"bounds": "1.0,2.0,3.0,4.0"}
      """;
    assertEquals(new Envelope(1.0, 3.0, 2.0, 4.0), jsonMapper.readValue(s1, TileArchiveMetadata.class).bounds());

    final String s2 = """
      {"bounds": "1.0,2.0,3.0,4.0,5.0"}
      """;
    assertEquals(new Envelope(1.0, 3.0, 2.0, 4.0), jsonMapper.readValue(s2, TileArchiveMetadata.class).bounds());
    assertThrows(JsonMappingException.class, () -> jsonMapperStrict.readValue(s2, TileArchiveMetadata.class));

    final String s3 = """
      {"bounds": "1.0"}
      """;
    assertNull(jsonMapper.readValue(s3, TileArchiveMetadata.class).bounds());
    assertThrows(JsonMappingException.class, () -> jsonMapperStrict.readValue(s3, TileArchiveMetadata.class));
  }

  @Test
  void testAddMetadataWorldBounds() {
    var bounds = GeoUtils.WORLD_LAT_LON_BOUNDS;
    var metadata = new TileArchiveMetadata(new Profile.NullProfile(), PlanetilerConfig.from(Arguments.of(Map.of(
      "bounds", bounds.getMinX() + "," + bounds.getMinY() + "," + bounds.getMaxX() + "," + bounds.getMaxY()
    ))));
    assertEquals(bounds, metadata.bounds());
    assertEquals(new CoordinateXY(0, 0), metadata.center());
    assertEquals(0d, metadata.zoom().doubleValue());
  }

  @Test
  void testAddMetadataSmallBounds() {
    var bounds = new Envelope(-73.6632, -69.7598, 41.1274, 43.0185);
    var metadata = new TileArchiveMetadata(new Profile.NullProfile(), PlanetilerConfig.from(Arguments.of(Map.of(
      "bounds", "-73.6632,41.1274,-69.7598,43.0185"
    ))));
    assertEquals(bounds, metadata.bounds());
    assertEquals(-71.7115, metadata.center().x, 1e-5);
    assertEquals(42.07295, metadata.center().y, 1e-5);
    assertEquals(7, Math.ceil(metadata.zoom()));
  }

  @Test
  void testToMap() throws JsonProcessingException {
    var bounds = "-73.6632,41.1274,-69.7598,43.0185";
    var metadata = new TileArchiveMetadata(
      new Profile.NullProfile(),
      PlanetilerConfig.from(Arguments.of(Map.of(
        "bounds", bounds
      ))));
    metadata = metadata.withLayerStats(
      List.of(
        new LayerAttrStats.VectorLayer(
          "aeroway",
          Map.of(
            "ref", LayerAttrStats.FieldType.STRING,
            "class", LayerAttrStats.FieldType.STRING
          ),
          12,
          14
        )
      )
    );
    var map = new TreeMap<>(metadata.toMap());
    assertNotNull(map.remove("planetiler:version"));
    map.remove("planetiler:githash");
    map.remove("planetiler:buildtime");
    TestUtils.assertSameJson(
      "[{\"id\":\"aeroway\",\"fields\":{\"ref\":\"String\",\"class\":\"String\"},\"minzoom\":12,\"maxzoom\":14}]",
      map.remove("vector_layers")
    );
    assertEquals(
      new TreeMap<>(Map.of(
        "name", "Null",
        "type", "baselayer",
        "format", "pbf",
        "zoom", "6.5271217861412305",
        "minzoom", "0",
        "maxzoom", "14",
        "bounds", "-73.6632,41.1274,-69.7598,43.0185",
        "center", "-71.7115,42.07295",
        "compression", "gzip"
      )),
      map
    );
  }

  private static void assertEqualsCoordinate(Coordinate c0, Coordinate c1) {
    assertEquals(c0, c1);
    assertTrue(c0.equals3D(c1)); // Coordinate#equals checks 2D only...
  }
}
