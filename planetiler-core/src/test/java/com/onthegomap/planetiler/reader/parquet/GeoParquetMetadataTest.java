package com.onthegomap.planetiler.reader.parquet;

import static com.onthegomap.planetiler.geo.GeoUtils.createMultiPoint;
import static com.onthegomap.planetiler.geo.GeoUtils.point;
import static org.apache.parquet.filter2.predicate.FilterApi.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.onthegomap.planetiler.config.Bounds;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.reader.WithTags;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.parquet.filter2.predicate.Filters;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.io.WKBWriter;
import org.locationtech.jts.io.WKTWriter;

class GeoParquetMetadataTest {
  // https://github.com/opengeospatial/geoparquet/blob/main/examples/example_metadata.json
  private static final String EXAMPLE_METADATA = """
     {
      "columns": {
        "geometry": {
          "bbox": [
            -180.0,
            -90.0,
            180.0,
            83.6451
          ],
          "covering": {
            "bbox": {
              "xmax": [
                "bbox",
                "xmax"
              ],
              "xmin": [
                "bbox",
                "xmin"
              ],
              "ymax": [
                "bbox",
                "ymax"
              ],
              "ymin": [
                "bbox",
                "ymin"
              ]
            }
          },
          "crs": {
            "$schema": "https://proj.org/schemas/v0.6/projjson.schema.json",
            "area": "World.",
            "bbox": {
              "east_longitude": 180,
              "north_latitude": 90,
              "south_latitude": -90,
              "west_longitude": -180
            },
            "coordinate_system": {
              "axis": [
                {
                  "abbreviation": "Lon",
                  "direction": "east",
                  "name": "Geodetic longitude",
                  "unit": "degree"
                },
                {
                  "abbreviation": "Lat",
                  "direction": "north",
                  "name": "Geodetic latitude",
                  "unit": "degree"
                }
              ],
              "subtype": "ellipsoidal"
            },
            "datum_ensemble": {
              "accuracy": "2.0",
              "ellipsoid": {
                "inverse_flattening": 298.257223563,
                "name": "WGS 84",
                "semi_major_axis": 6378137
              },
              "id": {
                "authority": "EPSG",
                "code": 6326
              },
              "members": [
                {
                  "id": {
                    "authority": "EPSG",
                    "code": 1166
                  },
                  "name": "World Geodetic System 1984 (Transit)"
                },
                {
                  "id": {
                    "authority": "EPSG",
                    "code": 1152
                  },
                  "name": "World Geodetic System 1984 (G730)"
                },
                {
                  "id": {
                    "authority": "EPSG",
                    "code": 1153
                  },
                  "name": "World Geodetic System 1984 (G873)"
                },
                {
                  "id": {
                    "authority": "EPSG",
                    "code": 1154
                  },
                  "name": "World Geodetic System 1984 (G1150)"
                },
                {
                  "id": {
                    "authority": "EPSG",
                    "code": 1155
                  },
                  "name": "World Geodetic System 1984 (G1674)"
                },
                {
                  "id": {
                    "authority": "EPSG",
                    "code": 1156
                  },
                  "name": "World Geodetic System 1984 (G1762)"
                },
                {
                  "id": {
                    "authority": "EPSG",
                    "code": 1309
                  },
                  "name": "World Geodetic System 1984 (G2139)"
                }
              ],
              "name": "World Geodetic System 1984 ensemble"
            },
            "id": {
              "authority": "OGC",
              "code": "CRS84"
            },
            "name": "WGS 84 (CRS84)",
            "scope": "Not known.",
            "type": "GeographicCRS"
          },
          "edges": "planar",
          "encoding": "WKB",
          "geometry_types": [
            "Polygon",
            "MultiPolygon"
          ]
        }
      },
      "primary_column": "geometry",
      "version": "1.1.0-dev"
    }
    """;

  @Test
  void testParseBasicMetadata() throws IOException {
    var parsed = GeoParquetMetadata.parse(new FileMetaData(
      Types.buildMessage()
        .required(PrimitiveType.PrimitiveTypeName.BINARY)
        .named("geometry")
        .named("root"),
      Map.of("geo", EXAMPLE_METADATA),
      ""));
    assertEquals("geometry", parsed.primaryColumn());
    assertEquals("1.1.0-dev", parsed.version());
    assertEquals("planar", parsed.primaryColumnMetadata().edges());
    assertEquals("WKB", parsed.primaryColumnMetadata().encoding());
    assertEquals(new Envelope(-180.0, 180.0, -90.0, 83.6451), parsed.primaryColumnMetadata().envelope());
    assertEquals(new GeoParquetMetadata.CoveringBbox(
      List.of("bbox", "xmin"),
      List.of("bbox", "ymin"),
      List.of("bbox", "xmax"),
      List.of("bbox", "ymax")
    ), parsed.primaryColumnMetadata().covering().bbox());
    assertEquals(List.of("Polygon", "MultiPolygon"), parsed.primaryColumnMetadata().geometryTypes());
    assertTrue(parsed.primaryColumnMetadata().crs() instanceof Map);
  }

  @Test
  void testFailsWhenNoGeometry() {
    var fileMetadata = new FileMetaData(
      Types.buildMessage()
        .required(PrimitiveType.PrimitiveTypeName.BINARY)
        .named("not_geometry")
        .named("root"),
      Map.of(),
      "");
    assertThrows(IOException.class, () -> GeoParquetMetadata.parse(fileMetadata));
  }

  @Test
  void testFailsWhenBadGeometryType() {
    var fileMetadata = new FileMetaData(
      Types.buildMessage()
        .required(PrimitiveType.PrimitiveTypeName.INT32)
        .named("geometry")
        .named("root"),
      Map.of(),
      "");
    assertThrows(IOException.class, () -> GeoParquetMetadata.parse(fileMetadata));
  }

  @Test
  void testInfersDefaultGeometry() throws IOException {
    var parsed = GeoParquetMetadata.parse(new FileMetaData(
      Types.buildMessage()
        .required(PrimitiveType.PrimitiveTypeName.BINARY)
        .named("geometry")
        .named("root"),
      Map.of(),
      ""));
    assertEquals("geometry", parsed.primaryColumn());
    assertEquals("WKB", parsed.primaryColumnMetadata().encoding());
    assertEquals(Bounds.WORLD.latLon(), parsed.primaryColumnMetadata().envelope());
    assertNull(parsed.primaryColumnMetadata().covering());
  }

  @Test
  void testGeometryReaderFromMetadata() throws IOException, GeometryException {
    var parsed = GeoParquetMetadata.parse(new FileMetaData(
      Types.buildMessage()
        .required(PrimitiveType.PrimitiveTypeName.BINARY)
        .named("geometry")
        .named("root"),
      Map.of("geo", EXAMPLE_METADATA),
      ""));
    assertEquals(point(1, 2), new GeometryReader(parsed).readPrimaryGeometry(WithTags.from(Map.of(
      "geometry", new WKBWriter().write(point(1, 2))
    ))));
  }

  @Test
  void testGeometryReaderFromMetadataDifferentName() throws IOException, GeometryException {
    var parsed = GeoParquetMetadata.parse(new FileMetaData(
      Types.buildMessage()
        .required(PrimitiveType.PrimitiveTypeName.BINARY)
        .named("other")
        .named("root"),
      Map.of("geo", """
        {
          "primary_column": "other",
          "columns": {
            "other": {
              "encoding": "WKB"
            }
          }
        }
        """),
      ""));
    assertEquals(point(1, 2), new GeometryReader(parsed).readPrimaryGeometry(WithTags.from(Map.of(
      "other", new WKBWriter().write(point(1, 2))
    ))));
  }

  @ParameterizedTest
  @ValueSource(strings = {"wkb_geometry", "geometry"})
  void testReadWKBGeometryNoMetadata(String name) throws IOException, GeometryException {
    var parsed = GeoParquetMetadata.parse(new FileMetaData(
      Types.buildMessage()
        .required(PrimitiveType.PrimitiveTypeName.BINARY)
        .named(name)
        .named("root"),
      Map.of(),
      ""));
    assertEquals(point(1, 2), new GeometryReader(parsed).readPrimaryGeometry(WithTags.from(Map.of(
      name, new WKBWriter().write(point(1, 2))
    ))));
  }

  @Test
  void testReadWKTGeometryNoMetadata() throws IOException, GeometryException {
    var parsed = GeoParquetMetadata.parse(new FileMetaData(
      Types.buildMessage()
        .required(PrimitiveType.PrimitiveTypeName.BINARY)
        .named("wkt_geometry")
        .named("root"),
      Map.of(),
      ""));
    assertEquals(point(1, 2), new GeometryReader(parsed).readPrimaryGeometry(WithTags.from(Map.of(
      "wkt_geometry", new WKTWriter().write(point(1, 2))
    ))));
  }

  @TestFactory
  void testReadGeoArrowPoint() throws IOException, GeometryException {
    var parsed = GeoParquetMetadata.parse(new FileMetaData(
      Types.buildMessage().named("root"),
      Map.of("geo", """
        {
          "primary_column": "geoarrow",
          "columns": {
            "geoarrow": {
              "encoding": "point"
            }
          }
        }
        """),
      ""));
    assertEquals(point(1, 2), new GeometryReader(parsed).readPrimaryGeometry(WithTags.from(Map.of(
      "geoarrow", Map.of("x", 1, "y", 2)
    ))));
  }

  @TestFactory
  void testReadGeoArrowMultiPoint() throws IOException, GeometryException {
    var parsed = GeoParquetMetadata.parse(new FileMetaData(
      Types.buildMessage().named("root"),
      Map.of("geo", """
        {
          "primary_column": "geoarrow",
          "columns": {
            "geoarrow": {
              "encoding": "multipolygon"
            }
          }
        }
        """),
      ""));
    assertEquals(createMultiPoint(List.of(point(1, 2))),
      new GeometryReader(parsed).readPrimaryGeometry(WithTags.from(Map.of(
        "geoarrow", List.of(Map.of("x", 1, "y", 2))
      ))));
  }

  @ParameterizedTest
  @CsvSource({
    "bbox, true, DOUBLE",
    "bbox, true, FLOAT",
    "custom_bbox, true, DOUBLE",
    "custom_bbox, true, FLOAT",
    "bbox, false, DOUBLE",
    "bbox, false, FLOAT",
  })
  void testBboxFilterFromMetadata(String bbox, boolean hasMetadata, PrimitiveType.PrimitiveTypeName type)
    throws IOException {
    var schema = Types.buildMessage()
      .required(PrimitiveType.PrimitiveTypeName.BINARY)
      .named("geometry")
      .requiredGroup()
      .required(type).named("xmin")
      .required(type).named("xmax")
      .required(type).named("ymin")
      .required(type).named("ymax")
      .named(bbox)
      .named("root");
    var parsed = GeoParquetMetadata.parse(new FileMetaData(
      schema,
      hasMetadata ? Map.of("geo", EXAMPLE_METADATA.replaceAll("\"bbox\",", "\"" + bbox + "\",")) : Map.of(),
      ""));
    var expected = type == PrimitiveType.PrimitiveTypeName.FLOAT ?
      and(
        and(gtEq(floatColumn(bbox + ".xmax"), 1f), ltEq(floatColumn(bbox + ".xmin"), 2f)),
        and(gtEq(floatColumn(bbox + ".ymax"), 3f), ltEq(floatColumn(bbox + ".ymin"), 4f))
      ) :
      and(
        and(gtEq(doubleColumn(bbox + ".xmax"), 1.0), ltEq(doubleColumn(bbox + ".xmin"), 2.0)),
        and(gtEq(doubleColumn(bbox + ".ymax"), 3.0), ltEq(doubleColumn(bbox + ".ymin"), 4.0))
      );
    assertEquals(expected, parsed.primaryColumnMetadata().bboxFilter(schema, new Bounds(new Envelope(1, 2, 3, 4))));
  }

  @ParameterizedTest
  @CsvSource({
    "bbox, true, DOUBLE",
    "bbox, true, FLOAT",
    "custom_bbox, true, DOUBLE",
    "custom_bbox, true, FLOAT",
    "bbox, false, DOUBLE",
    "bbox, false, FLOAT",
  })
  void testBboxFilterFromMetadataOldGdalStyle(String bbox, boolean hasMetadata, PrimitiveType.PrimitiveTypeName type)
    throws IOException {
    var schema = Types.buildMessage()
      .required(PrimitiveType.PrimitiveTypeName.BINARY)
      .named("geometry")
      .required(type).named(bbox + ".xmin")
      .required(type).named(bbox + ".xmax")
      .required(type).named(bbox + ".ymin")
      .required(type).named(bbox + ".ymax")
      .named("root");
    var parsed = GeoParquetMetadata.parse(new FileMetaData(
      schema,
      hasMetadata ? Map.of("geo", """
        {
          "primary_column": "geometry",
          "columns": {
            "geometry": {
              "covering": {
                "bbox": {
                  "xmin": ["bbox.xmin"],
                  "xmax": ["bbox.xmax"],
                  "ymin": ["bbox.ymin"],
                  "ymax": ["bbox.ymax"]
                }
              }
            }
          }
        }
        """.replace("bbox.", bbox + ".")) : Map.of(),
      ""));
    var expected = type == PrimitiveType.PrimitiveTypeName.FLOAT ?
      and(
        and(gtEq(Filters.floatColumn(List.of(bbox + ".xmax")), 1f),
          ltEq(Filters.floatColumn(List.of(bbox + ".xmin")), 2f)),
        and(gtEq(Filters.floatColumn(List.of(bbox + ".ymax")), 3f),
          ltEq(Filters.floatColumn(List.of(bbox + ".ymin")), 4f))
      ) :
      and(
        and(gtEq(Filters.doubleColumn(List.of(bbox + ".xmax")), 1.0),
          ltEq(Filters.doubleColumn(List.of(bbox + ".xmin")), 2.0)),
        and(gtEq(Filters.doubleColumn(List.of(bbox + ".ymax")), 3.0),
          ltEq(Filters.doubleColumn(List.of(bbox + ".ymin")), 4.0))
      );
    assertEquals(expected, parsed.primaryColumnMetadata().bboxFilter(schema, new Bounds(new Envelope(1, 2, 3, 4))));
  }

  @Test
  void testNoBboxFilterFromDefault() throws IOException {
    var schema = Types.buildMessage()
      .required(PrimitiveType.PrimitiveTypeName.BINARY)
      .named("geometry")
      .named("root");
    var parsed = GeoParquetMetadata.parse(new FileMetaData(
      schema,
      Map.of(),
      ""));
    assertNull(parsed.primaryColumnMetadata().bboxFilter(schema, new Bounds(new Envelope(1, 2, 3, 4))));
  }
}
