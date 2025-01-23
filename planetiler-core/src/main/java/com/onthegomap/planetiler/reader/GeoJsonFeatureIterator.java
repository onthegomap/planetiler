package com.onthegomap.planetiler.reader;

import static com.fasterxml.jackson.core.JsonParser.Feature.INCLUDE_SOURCE_IN_LOCATION;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.MutableCoordinateSequence;
import com.onthegomap.planetiler.util.CloseableIterator;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Polygon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GeoJsonFeatureIterator implements CloseableIterator<GeoJsonFeature> {
  private static final Logger LOGGER = LoggerFactory.getLogger(GeoJsonFeatureIterator.class);
  private final ObjectMapper mapper = new ObjectMapper();
  private final JsonParser parser;
  private GeoJsonFeature next = null;
  private Map<String, Object> properties = null;
  private GeoJsonGeometry geometry;
  private int nestingLevel = 0;

  @JsonIgnoreProperties(ignoreUnknown = true)
  private record GeoJsonGeometry(String type, List<?> coordinates) {}

  public GeoJsonFeatureIterator(InputStream in) throws IOException {
    this.parser = new JsonFactory().createParser(in);
    parser.enable(INCLUDE_SOURCE_IN_LOCATION);
    advance();
  }

  @Override
  public void close() {
    try {
      parser.close();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public boolean hasNext() {
    return next != null;
  }

  @Override
  public GeoJsonFeature next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    GeoJsonFeature item = next;
    advance();
    return item;
  }

  private void advance() {
    try {
      geometry = null;
      properties = null;
      next = null;
      JsonToken token = null;
      while (next == null && !parser.isClosed()) {
        if (nestingLevel == 0) {
          findNextStruct();
        }
        while (!parser.isClosed() && (nestingLevel > 0) && !(token = parser.nextToken()).isStructEnd()) {
          if (token == JsonToken.START_OBJECT) {
            nestingLevel++;
          } else if (token == JsonToken.FIELD_NAME) {
            String field = parser.currentName();
            switch (field) {
              case "geometry" -> consumeGeometry();
              case "properties" -> consumeProperties();
              case "type" -> consume(JsonToken.VALUE_STRING);
              case "features" -> {
                consume(JsonToken.START_ARRAY);
                nestingLevel++;
                consume(JsonToken.START_OBJECT);
                nestingLevel++;
              }
              case null, default -> {
                parser.nextToken();
                parser.skipChildren();
              }
            }
          } else {
            LOGGER.warn("Unexpected token inside struct at {}: {}", loc(), token);
          }
        }
        if (token == JsonToken.END_ARRAY) {
          nestingLevel--;
        } else if (token == JsonToken.END_OBJECT) {
          var geom = getGeometry();
          if (geom != null) {
            next = new GeoJsonFeature(geom, properties == null ? Map.of() : properties);
          }
          nestingLevel--;
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

  }

  private void consume(JsonToken tokenType) throws IOException {
    if (parser.nextToken() != tokenType) {
      LOGGER.warn("Unexpected token type at {}: {}", loc(), tokenType);
    }
  }

  private void findNextStruct() throws IOException {
    JsonToken token;
    while ((token = parser.nextToken()) != null && token != JsonToken.START_OBJECT) {
      LOGGER.warn("Unexpected top-level token at {}: {}", loc(), token);
      parser.skipChildren();
    }
    if (!parser.isClosed()) {
      nestingLevel++;
    }
  }

  private String loc() {
    return parser.currentTokenLocation().offsetDescription();
  }

  private Geometry getGeometry() {
    if (geometry == null) {
      return null;
    }
    return switch (geometry.type) {
      case "Point" -> GeoUtils.JTS_FACTORY.createPoint(coordinate(geometry.coordinates));
      case "LineString" -> GeoUtils.JTS_FACTORY.createLineString(coordinateSequence(geometry.coordinates));
      case "Polygon" -> polygon(geometry.coordinates);
      case "MultiPoint" -> GeoUtils.JTS_FACTORY.createMultiPoint(coordinateSequence(geometry.coordinates));
      case "MultiLineString" -> {
        List<CoordinateSequence> lines = coordinateSequenceList(geometry.coordinates);
        yield GeoUtils.createMultiLineString(lines.stream().map(GeoUtils.JTS_FACTORY::createLineString).toList());
      }
      case "MultiPolygon" -> {
        List<Polygon> polygons = geometry.coordinates.stream()
          .filter(List.class::isInstance)
          .map(List.class::cast)
          .map(this::polygon)
          .toList();
        yield GeoUtils.createMultiPolygon(polygons);
      }
      case null, default -> {
        LOGGER.warn("Unexpected geometry type: {}", geometry.type);
        yield null;
      }
    };
  }

  private Polygon polygon(List<?> list) {
    List<LinearRing> rings = linearRingList(list);
    return GeoUtils.createPolygon(rings.getFirst(), rings.subList(1, rings.size()));
  }

  private Coordinate coordinate(List<?> list) {
    return new CoordinateXY(((Number) list.get(0)).doubleValue(), ((Number) list.get(1)).doubleValue());
  }

  private CoordinateSequence coordinateSequence(List<?> list) {
    MutableCoordinateSequence result = new MutableCoordinateSequence(list.size());
    for (var item : list) {
      if (item instanceof List<?> coord) {
        result.addPoint(((Number) coord.get(0)).doubleValue(), ((Number) coord.get(1)).doubleValue());
      }
    }
    return result;
  }

  private LinearRing linearRing(List<?> list) {
    return GeoUtils.JTS_FACTORY.createLinearRing(coordinateSequence(list));
  }

  private List<LinearRing> linearRingList(List<?> list) {
    return list.stream().filter(List.class::isInstance).map(List.class::cast).map(this::linearRing).toList();
  }

  private List<List<LinearRing>> linearRingLists(List<?> list) {
    return list.stream().filter(List.class::isInstance).map(List.class::cast).map(this::linearRingList).toList();
  }

  private List<CoordinateSequence> coordinateSequenceList(List<?> list) {
    return list.stream().filter(List.class::isInstance).map(List.class::cast).map(this::coordinateSequence).toList();
  }

  private List<List<CoordinateSequence>> coordinateSequenceLists(List<?> list) {
    return list.stream().filter(List.class::isInstance).map(List.class::cast).map(this::coordinateSequenceList)
      .toList();
  }

  private void consumeProperties() throws IOException {
    parser.nextToken();
    properties = mapper.readValue(parser, Map.class);
  }

  private void consumeGeometry() throws IOException {
    parser.nextToken();
    geometry = mapper.readValue(parser, GeoJsonGeometry.class);
  }
}
