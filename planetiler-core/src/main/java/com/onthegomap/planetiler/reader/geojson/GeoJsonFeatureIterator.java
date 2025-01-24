package com.onthegomap.planetiler.reader.geojson;

import static com.fasterxml.jackson.core.JsonParser.Feature.INCLUDE_SOURCE_IN_LOCATION;
import static com.onthegomap.planetiler.geo.GeoUtils.JTS_FACTORY;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonLocation;
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
import java.util.function.Function;
import java.util.stream.Stream;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.CoordinateSequences;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Internal streaming geojson parsing utility.
 */
class GeoJsonFeatureIterator implements CloseableIterator<GeoJsonFeature> {

  private static final Logger LOGGER = LoggerFactory.getLogger(GeoJsonFeatureIterator.class);
  private final ObjectMapper mapper = new ObjectMapper();
  private final JsonParser parser;
  private GeoJsonFeature next = null;
  private Map<String, Object> properties = null;
  private GeoJsonGeometry geometry;
  private int nestingLevel = 0;
  private JsonLocation geometryStart;
  private int warnings = 0;
  private static final int WARN_LIMIT = 100;
  private final String name;

  @JsonIgnoreProperties(ignoreUnknown = true)
  private record GeoJsonGeometry(String type, Object coordinates) {}

  GeoJsonFeatureIterator(InputStream in, String name) throws IOException {
    this.name = name;
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
          switch (token) {
            case JsonToken.START_OBJECT -> nestingLevel++;
            case JsonToken.FIELD_NAME -> {
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
            }
            default -> warn("Unexpected token inside struct: " + token);
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

  private boolean consume(JsonToken tokenType) throws IOException {
    if (parser.nextToken() != tokenType) {
      warn("Unexpected token type: " + tokenType);
      parser.skipChildren();
      return false;
    }
    return true;
  }

  private void consumeProperties() throws IOException {
    consume(JsonToken.START_OBJECT);
    properties = mapper.readValue(parser, Map.class);
  }

  private void consumeGeometry() throws IOException {
    if (consume(JsonToken.START_OBJECT)) {
      geometryStart = parser.currentTokenLocation();
      geometry = mapper.readValue(parser, GeoJsonGeometry.class);
    }
  }

  private void findNextStruct() throws IOException {
    JsonToken token;
    while ((token = parser.nextToken()) != null && token != JsonToken.START_OBJECT) {
      warn("Unexpected top-level token: " + token);
      parser.skipChildren();
    }
    if (!parser.isClosed()) {
      nestingLevel++;
    }
  }

  private Geometry getGeometry() {
    if (geometry == null) {
      return null;
    }
    var factory = JTS_FACTORY;
    if (geometry.coordinates instanceof List<?> coords) {
      return switch (geometry.type) {
        case "Point" -> point(coords);
        case "LineString" -> lineString(coords);
        case "Polygon" -> polygon(coords);
        case "MultiPoint" -> factory.createMultiPoint(map(coords, this::point).toArray(Point[]::new));
        case "MultiLineString" ->
          factory.createMultiLineString(map(coords, this::lineString).toArray(LineString[]::new));
        case "MultiPolygon" -> factory.createMultiPolygon(map(coords, this::polygon).toArray(Polygon[]::new));
        case null, default -> {
          warnGeometry("Unexpected geometry type: " + geometry.type);
          yield GeoUtils.EMPTY_GEOMETRY;
        }
      };
    }
    return GeoUtils.EMPTY_GEOMETRY;
  }

  private <T> Stream<T> map(List<?> coordinates, Function<List<?>, T> mapper) {
    return coordinates.stream().filter(item -> {
      if (!(item instanceof List<?>)) {
        warnGeometry("Expecting list in geojson geometry but got: " + item);
        return false;
      }
      return true;
    }).<List<?>>map(List.class::cast).map(mapper);
  }

  private LineString lineString(List<?> coordinates) {
    return JTS_FACTORY.createLineString(coordinateSequence(coordinates));
  }

  private Point point(List<?> coordinates) {
    return JTS_FACTORY.createPoint(coordinate(coordinates));
  }

  private Polygon polygon(List<?> coordinates) {
    List<LinearRing> rings = linearRingList(coordinates);
    return GeoUtils.createPolygon(
      rings.isEmpty() ? null : rings.getFirst(),
      rings.size() <= 1 ? List.of() : rings.subList(1, rings.size())
    );
  }

  private CoordinateSequence coordinate(List<?> coord) {
    MutableCoordinateSequence result = new MutableCoordinateSequence();
    if (coord.size() >= 2 && coord.get(0) instanceof Number x && coord.get(1) instanceof Number y) {
      result.addPoint(x.doubleValue(), y.doubleValue());
    } else {
      warnGeometry("Invalid geojson point coordinate: " + coord);
    }
    return result;
  }

  private CoordinateSequence coordinateSequence(List<?> list) {
    MutableCoordinateSequence result = new MutableCoordinateSequence(list.size());
    for (var item : list) {
      if (item instanceof List<?> coord && coord.size() >= 2 &&
        coord.get(0) instanceof Number x && coord.get(1) instanceof Number y) {
        result.forceAddPoint(x.doubleValue(), y.doubleValue());
      } else {
        warnGeometry("Invalid geojson coordinate: " + item);
      }
    }
    return result;
  }

  private LinearRing linearRing(List<?> list) {
    var coordinateSequence = coordinateSequence(list);
    if (!CoordinateSequences.isRing(coordinateSequence)) {
      warnGeometry("Invalid geojson polygon ring " + list);
    }
    return CoordinateSequences.isRing(coordinateSequence) ?
      JTS_FACTORY.createLinearRing(coordinateSequence(list)) : JTS_FACTORY.createLinearRing();
  }

  private List<LinearRing> linearRingList(List<?> list) {
    return list.stream().filter(List.class::isInstance).map(List.class::cast).map(this::linearRing).toList();
  }

  private void warn(String message) {
    warn(message, parser.currentTokenLocation());
  }

  private void warnGeometry(String message) {
    warn(message, geometryStart);
  }

  private void warn(String message, JsonLocation ref) {
    if (ref == null) {
      ref = parser.currentTokenLocation();
    }
    if (++warnings < WARN_LIMIT) {
      LOGGER.warn("[{}{}:{}] {}", name == null ? "" : (name + ":"), ref.getLineNr(), ref.getColumnNr(), message);
    } else if (warnings == WARN_LIMIT) {
      LOGGER.warn("Too many warnings, geojson file might be invalid");
    }
  }
}
