/* ****************************************************************
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 ****************************************************************/
package com.onthegomap.planetiler;

import com.carrotsearch.hppc.IntArrayList;
import com.google.common.primitives.Ints;
import com.google.protobuf.InvalidProtocolBufferException;
import com.onthegomap.planetiler.collection.FeatureGroup;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.geo.GeometryType;
import com.onthegomap.planetiler.geo.MutableCoordinateSequence;
import com.onthegomap.planetiler.util.Hilbert;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.concurrent.NotThreadSafe;
import org.locationtech.jts.algorithm.Orientation;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.Puntal;
import org.locationtech.jts.geom.impl.CoordinateArraySequence;
import org.locationtech.jts.geom.impl.PackedCoordinateSequence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vector_tile.VectorTileProto;

/**
 * Encodes a single output tile containing JTS {@link Geometry} features into the compact binary Mapbox Vector Tile
 * format.
 * <p>
 * This class is copied from <a href=
 * "https://github.com/ElectronicChartCentre/java-vector-tile/blob/master/src/main/java/no/ecc/vectortile/VectorTileEncoder.java">VectorTileEncoder.java</a>
 * and <a href=
 * "https://github.com/ElectronicChartCentre/java-vector-tile/blob/master/src/main/java/no/ecc/vectortile/VectorTileDecoder.java">VectorTileDecoder.java</a>
 * and modified to decouple geometry encoding from vector tile encoding so that encoded commands can be stored in the
 * sorted feature map prior to encoding vector tiles. The internals are also refactored to improve performance by using
 * hppc primitive collections.
 *
 * @see <a href="https://github.com/mapbox/vector-tile-spec/tree/master/2.1">Mapbox Vector Tile Specification</a>
 */
@NotThreadSafe
public class VectorTile {

  private static final Logger LOGGER = LoggerFactory.getLogger(VectorTile.class);

  // TODO make these configurable
  private static final int EXTENT = 4096;
  private static final double SIZE = 256d;
  private final Map<String, Layer> layers = new LinkedHashMap<>();

  private static int[] getCommands(Geometry input, int scale) {
    var encoder = new CommandEncoder(scale);
    encoder.accept(input);
    return encoder.result.toArray();
  }

  /**
   * Scales a geometry down by a factor of {@code 2^scale} without materializing an intermediate JTS geometry and
   * returns the encoded result.
   */
  private static int[] unscale(int[] commands, int scale, GeometryType geomType) {
    IntArrayList result = new IntArrayList();
    int geometryCount = commands.length;
    int length = 0;
    int command = 0;
    int i = 0;
    int inX = 0, inY = 0;
    int outX = 0, outY = 0;
    int startX = 0, startY = 0;
    double scaleFactor = Math.pow(2, -scale);
    int lengthIdx = 0;
    int moveToIdx = 0;
    int pointsInShape = 0;
    boolean first = true;
    while (i < geometryCount) {
      if (length <= 0) {
        length = commands[i++];
        lengthIdx = result.size();
        result.add(length);
        command = length & ((1 << 3) - 1);
        length = length >> 3;
      }

      if (length > 0) {
        if (command == Command.MOVE_TO.value) {
          // degenerate geometry, remove it from output entirely
          if (!first && pointsInShape < geomType.minPoints()) {
            int prevCommand = result.get(lengthIdx);
            result.elementsCount = moveToIdx;
            result.add(prevCommand);
            // reset deltas
            outX = startX;
            outY = startY;
          }
          // keep track of size of next shape...
          pointsInShape = 0;
          startX = outX;
          startY = outY;
          moveToIdx = result.size() - 1;
        }
        first = false;
        if (command == Command.CLOSE_PATH.value) {
          pointsInShape++;
          length--;
          continue;
        }

        int dx = commands[i++];
        int dy = commands[i++];

        length--;

        dx = zigZagDecode(dx);
        dy = zigZagDecode(dy);

        inX = inX + dx;
        inY = inY + dy;

        int nextX = (int) Math.round(inX * scaleFactor);
        int nextY = (int) Math.round(inY * scaleFactor);

        if (nextX == outX && nextY == outY && command == Command.LINE_TO.value) {
          int commandLength = result.get(lengthIdx) - 8;
          if (commandLength < 8) {
            // get rid of lineto section if empty
            result.elementsCount = lengthIdx;
          } else {
            result.set(lengthIdx, commandLength);
          }
        } else {
          pointsInShape++;
          int dxOut = nextX - outX;
          int dyOut = nextY - outY;
          result.add(
            zigZagEncode(dxOut),
            zigZagEncode(dyOut)
          );
          outX = nextX;
          outY = nextY;
        }
      }
    }
    // degenerate geometry, remove it from output entirely
    if (pointsInShape < geomType.minPoints()) {
      result.elementsCount = moveToIdx;
    }
    return result.toArray();
  }

  static int zigZagEncode(int n) {
    // https://developers.google.com/protocol-buffers/docs/encoding#types
    return (n << 1) ^ (n >> 31);
  }

  private static int zigZagDecode(int n) {
    // https://developers.google.com/protocol-buffers/docs/encoding#types
    return ((n >> 1) ^ (-(n & 1)));
  }

  private static Geometry decodeCommands(GeometryType geomType, int[] commands, int scale) throws GeometryException {
    try {
      GeometryFactory gf = GeoUtils.JTS_FACTORY;
      double SCALE = (EXTENT << scale) / SIZE;
      int x = 0;
      int y = 0;

      List<MutableCoordinateSequence> allCoordSeqs = new ArrayList<>();
      MutableCoordinateSequence currentCoordSeq = null;

      int geometryCount = commands.length;
      int length = 0;
      int command = 0;
      int i = 0;
      while (i < geometryCount) {

        if (length <= 0) {
          length = commands[i++];
          command = length & ((1 << 3) - 1);
          length = length >> 3;
          assert geomType != GeometryType.POINT || i == 1 : "Invalid multipoint, command found at index %d, expected 0"
            .formatted(i);
          assert geomType != GeometryType.POINT ||
            (length * 2 + 1 == geometryCount) : "Invalid multipoint: int[%d] length=%d".formatted(geometryCount,
              length);
        }

        if (length > 0) {

          if (command == Command.MOVE_TO.value) {
            currentCoordSeq = new MutableCoordinateSequence();
            allCoordSeqs.add(currentCoordSeq);
          } else {
            assert currentCoordSeq != null;
          }

          if (command == Command.CLOSE_PATH.value) {
            if (geomType != GeometryType.POINT && !currentCoordSeq.isEmpty()) {
              currentCoordSeq.closeRing();
            }
            length--;
            continue;
          }

          int dx = commands[i++];
          int dy = commands[i++];

          length--;

          dx = zigZagDecode(dx);
          dy = zigZagDecode(dy);

          x = x + dx;
          y = y + dy;

          currentCoordSeq.forceAddPoint(x / SCALE, y / SCALE);
        }

      }

      Geometry geometry = null;
      boolean outerCCW = false;

      switch (geomType) {
        case LINE -> {
          List<LineString> lineStrings = new ArrayList<>(allCoordSeqs.size());
          for (MutableCoordinateSequence coordSeq : allCoordSeqs) {
            if (coordSeq.size() <= 1) {
              continue;
            }
            lineStrings.add(gf.createLineString(coordSeq));
          }
          if (lineStrings.size() == 1) {
            geometry = lineStrings.get(0);
          } else if (lineStrings.size() > 1) {
            geometry = gf.createMultiLineString(lineStrings.toArray(new LineString[0]));
          }
        }
        case POINT -> {
          CoordinateSequence cs = new PackedCoordinateSequence.Double(allCoordSeqs.size(), 2, 0);
          for (int j = 0; j < allCoordSeqs.size(); j++) {
            MutableCoordinateSequence coordSeq = allCoordSeqs.get(j);
            cs.setOrdinate(j, 0, coordSeq.getX(0));
            cs.setOrdinate(j, 1, coordSeq.getY(0));
          }
          if (cs.size() == 1) {
            geometry = gf.createPoint(cs);
          } else if (cs.size() > 1) {
            geometry = gf.createMultiPoint(cs);
          }
        }
        case POLYGON -> {
          List<List<LinearRing>> polygonRings = new ArrayList<>();
          List<LinearRing> ringsForCurrentPolygon = new ArrayList<>();
          boolean first = true;
          for (MutableCoordinateSequence coordSeq : allCoordSeqs) {
            // skip hole with too few coordinates
            if (ringsForCurrentPolygon.size() > 0 && coordSeq.size() < 2) {
              continue;
            }
            LinearRing ring = gf.createLinearRing(coordSeq);
            boolean ccw = Orientation.isCCW(coordSeq);
            if (first) {
              first = false;
              outerCCW = ccw;
              assert outerCCW : "outer ring is not counter-clockwise";
            }
            if (ccw == outerCCW) {
              ringsForCurrentPolygon = new ArrayList<>();
              polygonRings.add(ringsForCurrentPolygon);
            }
            ringsForCurrentPolygon.add(ring);
          }
          List<Polygon> polygons = new ArrayList<>();
          for (List<LinearRing> rings : polygonRings) {
            LinearRing shell = rings.get(0);
            LinearRing[] holes = rings.subList(1, rings.size()).toArray(new LinearRing[rings.size() - 1]);
            polygons.add(gf.createPolygon(shell, holes));
          }
          if (polygons.size() == 1) {
            geometry = polygons.get(0);
          }
          if (polygons.size() > 1) {
            geometry = gf.createMultiPolygon(GeometryFactory.toPolygonArray(polygons));
          }
        }
        default -> {
        }
      }

      if (geometry == null) {
        geometry = GeoUtils.EMPTY_GEOMETRY;
      }

      return geometry;
    } catch (IllegalArgumentException e) {
      throw new GeometryException("decode_vector_tile", "Unable to decode geometry", e);
    }
  }

  /**
   * Parses a binary-encoded vector tile protobuf into a list of features.
   * <p>
   * Does not decode geometries, but clients can call {@link VectorGeometry#decode()} to decode a JTS {@link Geometry}
   * if needed.
   * <p>
   * If {@code encoded} is compressed, clients must decompress it first.
   *
   * @param encoded encoded vector tile protobuf
   * @return list of features on that tile
   * @throws IllegalStateException     if decoding fails
   * @throws IndexOutOfBoundsException if a tag's key or value refers to an index that does not exist in the keys/values
   *                                   array for a layer
   */
  public static List<Feature> decode(byte[] encoded) {
    try {
      VectorTileProto.Tile tile = VectorTileProto.Tile.parseFrom(encoded);
      List<Feature> features = new ArrayList<>();
      for (VectorTileProto.Tile.Layer layer : tile.getLayersList()) {
        String layerName = layer.getName();
        assert layer.getExtent() == 4096;
        List<String> keys = layer.getKeysList();
        List<Object> values = new ArrayList<>();

        for (VectorTileProto.Tile.Value value : layer.getValuesList()) {
          if (value.hasBoolValue()) {
            values.add(value.getBoolValue());
          } else if (value.hasDoubleValue()) {
            values.add(value.getDoubleValue());
          } else if (value.hasFloatValue()) {
            values.add(value.getFloatValue());
          } else if (value.hasIntValue()) {
            values.add(value.getIntValue());
          } else if (value.hasSintValue()) {
            values.add(value.getSintValue());
          } else if (value.hasUintValue()) {
            values.add(value.getUintValue());
          } else if (value.hasStringValue()) {
            values.add(value.getStringValue());
          } else {
            values.add(null);
          }
        }

        for (VectorTileProto.Tile.Feature feature : layer.getFeaturesList()) {
          int tagsCount = feature.getTagsCount();
          Map<String, Object> attrs = new HashMap<>(tagsCount / 2);
          int tagIdx = 0;
          while (tagIdx < feature.getTagsCount()) {
            String key = keys.get(feature.getTags(tagIdx++));
            Object value = values.get(feature.getTags(tagIdx++));
            attrs.put(key, value);
          }
          features.add(new Feature(
            layerName,
            feature.getId(),
            new VectorGeometry(Ints.toArray(feature.getGeometryList()), GeometryType.valueOf(feature.getType()), 0),
            attrs
          ));
        }
      }
      return features;
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Encodes a JTS geometry according to
   * <a href="https://github.com/mapbox/vector-tile-spec/tree/master/2.1#43-geometry-encoding">Geometry Encoding
   * Specification</a>.
   *
   * @param geometry the JTS geometry to encoded
   * @return the geometry type and command array for the encoded geometry
   */
  public static VectorGeometry encodeGeometry(Geometry geometry) {
    return encodeGeometry(geometry, 0);
  }

  public static VectorGeometry encodeGeometry(Geometry geometry, int scale) {
    return new VectorGeometry(getCommands(geometry, scale), GeometryType.typeOf(geometry), scale);
  }

  /**
   * Returns a new {@link VectorGeometryMerger} that combines encoded geometries of the same type into a merged
   * multipoint, multilinestring, or multipolygon.
   */
  public static VectorGeometryMerger newMerger(GeometryType geometryType) {
    return new VectorGeometryMerger(geometryType);
  }

  /**
   * Returns the hilbert index of the zig-zag-encoded first point of {@code geometry}.
   * <p>
   * This can be useful for sorting geometries to minimize encoded vector tile geometry command size since smaller
   * offsets take fewer bytes using protobuf varint encoding.
   */
  public static int hilbertIndex(Geometry geometry) {
    Coordinate coord = geometry.getCoordinate();
    int x = zigZagEncode((int) Math.round(coord.x * 4096 / 256));
    int y = zigZagEncode((int) Math.round(coord.y * 4096 / 256));
    return Hilbert.hilbertXYToIndex(15, x, y);
  }

  /**
   * Returns the number of internal geometries in this feature including points/lines/polygons inside multigeometries.
   */
  public static int countGeometries(VectorTileProto.Tile.Feature feature) {
    int result = 0;
    int idx = 0;
    int geomCount = feature.getGeometryCount();
    while (idx < geomCount) {
      int length = feature.getGeometry(idx);
      int command = length & ((1 << 3) - 1);
      length = length >> 3;
      if (command == Command.MOVE_TO.value) {
        result += length;
      }
      idx += 1;
      if (command != Command.CLOSE_PATH.value) {
        idx += length * 2;
      }
    }
    return result;
  }

  /**
   * Adds features in a layer to this tile.
   *
   * @param layerName name of the layer in this tile to add the features to
   * @param features  features to add to the tile
   * @return this encoder for chaining
   */
  public VectorTile addLayerFeatures(String layerName, List<Feature> features) {
    if (features.isEmpty()) {
      return this;
    }

    Layer layer = layers.get(layerName);
    if (layer == null) {
      layer = new Layer();
      layers.put(layerName, layer);
    }

    for (Feature inFeature : features) {
      if (inFeature != null && inFeature.geometry().commands().length > 0) {
        EncodedFeature outFeature = new EncodedFeature(inFeature);

        for (Map.Entry<String, ?> e : inFeature.attrs().entrySet()) {
          // skip attribute without value
          if (e.getValue() != null) {
            outFeature.tags.add(layer.key(e.getKey()));
            outFeature.tags.add(layer.value(e.getValue()));
          }
        }

        layer.encodedFeatures.add(outFeature);
      }
    }
    return this;
  }

  /**
   * Returns a vector tile protobuf object with all features in this tile.
   */
  public VectorTileProto.Tile toProto() {
    VectorTileProto.Tile.Builder tile = VectorTileProto.Tile.newBuilder();
    for (Map.Entry<String, Layer> e : layers.entrySet()) {
      String layerName = e.getKey();
      Layer layer = e.getValue();

      VectorTileProto.Tile.Layer.Builder tileLayer = VectorTileProto.Tile.Layer.newBuilder()
        .setVersion(2)
        .setName(layerName)
        .setExtent(EXTENT)
        .addAllKeys(layer.keys());

      for (Object value : layer.values()) {
        VectorTileProto.Tile.Value.Builder tileValue = VectorTileProto.Tile.Value.newBuilder();
        switch (value) {
          case String stringValue -> tileValue.setStringValue(stringValue);
          case Integer intValue -> tileValue.setSintValue(intValue);
          case Long longValue -> tileValue.setSintValue(longValue);
          case Float floatValue -> tileValue.setFloatValue(floatValue);
          case Double doubleValue -> tileValue.setDoubleValue(doubleValue);
          case Boolean booleanValue -> tileValue.setBoolValue(booleanValue);
          case Object ignored -> tileValue.setStringValue(value.toString());
        }
        tileLayer.addValues(tileValue.build());
      }

      for (EncodedFeature feature : layer.encodedFeatures) {
        VectorTileProto.Tile.Feature.Builder featureBuilder = VectorTileProto.Tile.Feature.newBuilder()
          .addAllTags(Ints.asList(feature.tags.toArray()))
          .setType(feature.geometry().geomType().asProtobufType())
          .addAllGeometry(Ints.asList(feature.geometry().commands()));

        if (feature.id >= 0) {
          featureBuilder.setId(feature.id);
        }

        tileLayer.addFeatures(featureBuilder.build());
      }

      tile.addLayers(tileLayer.build());
    }
    return tile.build();
  }

  /**
   * Creates a vector tile protobuf with all features in this tile and serializes it as a byte array.
   * <p>
   * Does not compress the result.
   */
  public byte[] encode() {
    return toProto().toByteArray();
  }

  /**
   * Returns true if this tile contains only polygon fills.
   */
  public boolean containsOnlyFills() {
    return containsOnlyFillsOrEdges(false);
  }

  /**
   * Returns true if this tile contains only polygon fills or horizontal/vertical edges that are likely to be repeated
   * across tiles.
   */
  public boolean containsOnlyFillsOrEdges() {
    return containsOnlyFillsOrEdges(true);
  }

  private boolean containsOnlyFillsOrEdges(boolean allowEdges) {
    boolean empty = true;
    for (var layer : layers.values()) {
      for (var feature : layer.encodedFeatures) {
        empty = false;
        if (!feature.geometry.isFillOrEdge(allowEdges)) {
          return false;
        }
      }
    }
    return !empty;
  }

  /**
   * Determine whether a tile is likely to be a duplicate of some other tile hence it makes sense to calculate a hash
   * for it.
   * <p>
   * Deduplication code is aiming for a balance between filtering-out all duplicates and not spending too much CPU on
   * hash calculations: calculating hashes for all tiles costs too much CPU, not calculating hashes at all means
   * generating archives which are too big. This method is responsible for achieving that balance.
   * <p>
   * Current understanding is, that for the whole planet, there are 267m total tiles and 38m unique tiles. The
   * {@link #containsOnlyFillsOrEdges()} heuristic catches >99.9% of repeated tiles and cuts down the number of tile
   * hashes we need to track by 98% (38m to 735k). So it is considered a good tradeoff.
   *
   * @return {@code true} if the tile might have duplicates hence we want to calculate a hash for it
   */
  public boolean likelyToBeDuplicated() {
    return layers.values().stream().allMatch(v -> v.encodedFeatures.isEmpty()) || containsOnlyFillsOrEdges();
  }

  enum Command {
    MOVE_TO(1),
    LINE_TO(2),
    CLOSE_PATH(7);

    final int value;

    Command(int value) {
      this.value = value;
    }
  }

  /**
   * Utility that combines encoded geometries of the same type into a merged multipoint, multilinestring, or
   * multipolygon.
   */
  public static class VectorGeometryMerger implements Consumer<VectorGeometry> {
    // For the most part this just concatenates the individual command arrays together
    // EXCEPT we need to adjust the first coordinate of each subsequent linestring to
    // be an offset from the end of the previous linestring.
    // AND we need to combine all multipoint "move to" commands into one at the start of
    // the sequence

    private final GeometryType geometryType;
    private final IntArrayList result = new IntArrayList();
    private int overallX = 0;
    private int overallY = 0;

    private VectorGeometryMerger(GeometryType geometryType) {
      this.geometryType = geometryType;
    }

    @Override
    public void accept(VectorGeometry vectorGeometry) {
      if (vectorGeometry.geomType != geometryType) {
        throw new IllegalArgumentException(
          "Cannot merge a " + vectorGeometry.geomType.name().toLowerCase(Locale.ROOT) + " geometry into a multi" +
            vectorGeometry.geomType.name().toLowerCase(Locale.ROOT));
      }
      if (vectorGeometry.isEmpty()) {
        return;
      }
      var commands = vectorGeometry.unscale().commands();
      int x = 0;
      int y = 0;

      int geometryCount = commands.length;
      int length = 0;
      int command = 0;
      int i = 0;

      result.ensureCapacity(result.elementsCount + commands.length);
      // and multipoints will end up with only one command ("move to" with length=# points)
      if (geometryType != GeometryType.POINT || result.isEmpty()) {
        result.add(commands[0]);
      }
      result.add(zigZagEncode(zigZagDecode(commands[1]) - overallX));
      result.add(zigZagEncode(zigZagDecode(commands[2]) - overallY));
      if (commands.length > 3) {
        result.add(commands, 3, commands.length - 3);
      }

      while (i < geometryCount) {
        if (length <= 0) {
          length = commands[i++];
          command = length & ((1 << 3) - 1);
          length = length >> 3;
        }

        if (length > 0) {
          length--;
          if (command != Command.CLOSE_PATH.value) {
            x += zigZagDecode(commands[i++]);
            y += zigZagDecode(commands[i++]);
          }
        }
      }
      overallX = x;
      overallY = y;
    }

    /** Returns the merged multi-geometry. */
    public VectorGeometry finish() {
      // set the correct "move to" length for multipoints based on how many points were actually added
      if (geometryType == GeometryType.POINT) {
        result.buffer[0] = Command.MOVE_TO.value | (((result.size() - 1) / 2) << 3);
      }
      return new VectorGeometry(result.toArray(), geometryType, 0);
    }
  }

  /**
   * A vector geometry encoded as a list of commands according to the
   * <a href="https://github.com/mapbox/vector-tile-spec/tree/master/2.1#43-geometry-encoding">vector tile
   * specification</a>.
   * <p>
   * To encode extra precision in intermediate feature geometries, the geometry contained in {@code commands} is scaled
   * to a tile extent of {@code EXTENT * 2^scale}, so when the {@code scale == 0} the extent is {@link #EXTENT} and when
   * {@code scale == 2} the extent is 4x{@link #EXTENT}. Geometries must be scaled back to 0 using {@link #unscale()}
   * before outputting to the archive.
   */
  public record VectorGeometry(int[] commands, GeometryType geomType, int scale) {

    private static final int LEFT = 1;
    private static final int RIGHT = 1 << 1;
    private static final int TOP = 1 << 2;
    private static final int BOTTOM = 1 << 3;
    private static final int INSIDE = 0;
    private static final int ALL = TOP | LEFT | RIGHT | BOTTOM;
    private static final VectorGeometry EMPTY_POINT = new VectorGeometry(new int[0], GeometryType.POINT, 0);

    public VectorGeometry {
      if (scale < 0) {
        throw new IllegalArgumentException("scale can not be less than 0, got: " + scale);
      }
    }

    private static int getSide(int x, int y, int extent) {
      int result = INSIDE;
      if (x < 0) {
        result |= LEFT;
      } else if (x > extent) {
        result |= RIGHT;
      }
      if (y < 0) {
        result |= TOP;
      } else if (y > extent) {
        result |= BOTTOM;
      }
      return result;
    }

    private static boolean slanted(int x1, int y1, int x2, int y2) {
      return x1 != x2 && y1 != y2;
    }

    private static boolean segmentCrossesTile(int x1, int y1, int x2, int y2, int extent) {
      return (y1 >= 0 || y2 >= 0) &&
        (y1 <= extent || y2 <= extent) &&
        (x1 >= 0 || x2 >= 0) &&
        (x1 <= extent || x2 <= extent);
    }

    private static boolean isSegmentInvalid(boolean allowEdges, int x1, int y1, int x2, int y2, int extent) {
      boolean crossesTile = segmentCrossesTile(x1, y1, x2, y2, extent);
      if (allowEdges) {
        return crossesTile && slanted(x1, y1, x2, y2);
      } else {
        return crossesTile;
      }
    }


    private static boolean visitedEnoughSides(boolean allowEdges, int sides) {
      if (allowEdges) {
        return ((sides & LEFT) > 0 && (sides & RIGHT) > 0) || ((sides & TOP) > 0 && (sides & BOTTOM) > 0);
      } else {
        return sides == ALL;
      }
    }

    /** Converts an encoded geometry back to a JTS geometry. */
    public Geometry decode() throws GeometryException {
      return decodeCommands(geomType, commands, scale);
    }

    /** Returns this encoded geometry, scaled back to 0, so it is safe to emit to archive output. */
    public VectorGeometry unscale() {
      return scale == 0 ? this : new VectorGeometry(VectorTile.unscale(commands, scale, geomType), geomType, 0);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      VectorGeometry that = (VectorGeometry) o;

      if (geomType != that.geomType) {
        return false;
      }
      return Arrays.equals(commands, that.commands);
    }

    @Override
    public int hashCode() {
      int result = Arrays.hashCode(commands);
      result = 31 * result + geomType.hashCode();
      return result;
    }

    @Override
    public String toString() {
      return "VectorGeometry[" +
        "commands=int[" + commands.length +
        "], geomType=" + geomType +
        " (" + geomType.asByte() + ")]";
    }

    /** Returns true if the encoded geometry is a polygon fill. */
    public boolean isFill() {
      return isFillOrEdge(false);
    }

    /**
     * Returns true if the encoded geometry is a polygon fill, rectangle edge, or part of a horizontal/vertical line
     * that is likely to be repeated across tiles.
     */
    public boolean isFillOrEdge() {
      return isFillOrEdge(true);
    }

    /**
     * Returns true if the encoded geometry is a polygon fill, or if {@code allowEdges == true} then also a rectangle
     * edge, or part of a horizontal/vertical line that is likely to be repeated across tiles.
     */
    public boolean isFillOrEdge(boolean allowEdges) {
      if (geomType != GeometryType.POLYGON && (!allowEdges || geomType != GeometryType.LINE)) {
        return false;
      }

      boolean isLine = geomType == GeometryType.LINE;

      int extent = EXTENT << scale;
      int visited = INSIDE;
      int firstX = 0;
      int firstY = 0;
      int x = 0;
      int y = 0;

      int geometryCount = commands.length;
      int length = 0;
      int command = 0;
      int i = 0;
      while (i < geometryCount) {

        if (length <= 0) {
          length = commands[i++];
          command = length & ((1 << 3) - 1);
          length = length >> 3;
          if (isLine && length > 2) {
            return false;
          }
        }

        if (length > 0) {
          if (command == Command.CLOSE_PATH.value) {
            if (isSegmentInvalid(allowEdges, x, y, firstX, firstY, extent) ||
              !visitedEnoughSides(allowEdges, visited)) {
              return false;
            }
            length--;
            continue;
          }

          int dx = commands[i++];
          int dy = commands[i++];

          length--;

          dx = zigZagDecode(dx);
          dy = zigZagDecode(dy);

          int nextX = x + dx;
          int nextY = y + dy;

          if (command == Command.MOVE_TO.value) {
            firstX = nextX;
            firstY = nextY;
            if ((visited = getSide(firstX, firstY, extent)) == INSIDE) {
              return false;
            }
          } else {
            if (isSegmentInvalid(allowEdges, x, y, nextX, nextY, extent)) {
              return false;
            }
            visited |= getSide(nextX, nextY, extent);
          }
          y = nextY;
          x = nextX;
        }

      }

      return visitedEnoughSides(allowEdges, visited);
    }

    /** Returns true if there are no commands in this geometry. */
    public boolean isEmpty() {
      return commands.length == 0;
    }

    /**
     * If this is a point, returns an empty geometry if more than {@code buffer} pixels outside the tile bounds, or if
     * it is a multipoint than removes all points outside the buffer.
     */
    public VectorGeometry filterPointsOutsideBuffer(double buffer) {
      if (geomType != GeometryType.POINT) {
        return this;
      }
      IntArrayList result = null;

      int extent = (EXTENT << scale);
      int bufferInt = (int) Math.ceil(buffer * extent / 256);
      int min = -bufferInt;
      int max = extent + bufferInt;

      int x = 0;
      int y = 0;
      int lastX = 0;
      int lastY = 0;

      int geometryCount = commands.length;
      int length = 0;
      int i = 0;

      while (i < geometryCount) {
        if (length <= 0) {
          length = commands[i++] >> 3;
          assert i <= 1 : "Bad index " + i;
        }

        if (length > 0) {
          length--;
          x += zigZagDecode(commands[i++]);
          y += zigZagDecode(commands[i++]);
          if (x < min || y < min || x > max || y > max) {
            if (result == null) {
              // short-circuit the common case of only a single point that gets filtered-out
              if (commands.length == 3) {
                return EMPTY_POINT;
              }
              result = new IntArrayList(commands.length);
              result.add(commands, 0, i - 2);
            }
          } else {
            if (result != null) {
              result.add(zigZagEncode(x - lastX), zigZagEncode(y - lastY));
            }
            lastX = x;
            lastY = y;
          }
        }
      }
      if (result != null) {
        if (result.size() < 3) {
          result.elementsCount = 0;
        } else {
          result.set(0, Command.MOVE_TO.value | (((result.size() - 1) / 2) << 3));
        }

        return new VectorGeometry(result.toArray(), geomType, scale);
      } else {
        return this;
      }
    }

    /**
     * Returns the hilbert index of the zig-zag-encoded first point of this feature.
     * <p>
     * This can be useful for sorting geometries to minimize encoded vector tile geometry command size since smaller
     * offsets take fewer bytes using protobuf varint encoding.
     */
    public int hilbertIndex() {
      if (commands.length < 3) {
        return 0;
      }
      int x = commands[1];
      int y = commands[2];
      return Hilbert.hilbertXYToIndex(15, x >> scale, y >> scale);
    }

  }

  /**
   * A feature in a vector tile.
   *
   * @param layer    the layer the feature was in
   * @param id       the feature ID
   * @param geometry the encoded feature geometry (decode using {@link VectorGeometry#decode()})
   * @param attrs    tags for the feature to output
   * @param group    grouping key used to limit point density or {@link #NO_GROUP} if not in a group. NOTE: this is only
   *                 populated when this feature was deserialized from {@link FeatureGroup}, not when parsed from a tile
   *                 since vector tile schema does not encode group.
   */
  public record Feature(
    String layer,
    long id,
    VectorGeometry geometry,
    Map<String, Object> attrs,
    long group
  ) {

    public static final long NO_GROUP = Long.MIN_VALUE;

    public Feature(
      String layer,
      long id,
      VectorGeometry geometry,
      Map<String, Object> attrs
    ) {
      this(layer, id, geometry, attrs, NO_GROUP);
    }

    public boolean hasGroup() {
      return group != NO_GROUP;
    }

    /**
     * Encodes {@code newGeometry} and returns a copy of this feature with {@code geometry} replaced with the encoded
     * new geometry.
     */
    public Feature copyWithNewGeometry(Geometry newGeometry) {
      return copyWithNewGeometry(encodeGeometry(newGeometry));
    }

    /**
     * Returns a copy of this feature with {@code geometry} replaced with {@code newGeometry}.
     */
    public Feature copyWithNewGeometry(VectorGeometry newGeometry) {
      return newGeometry == geometry ? this : new Feature(
        layer,
        id,
        newGeometry,
        attrs,
        group
      );
    }

    /** Returns a copy of this feature with {@code extraAttrs} added to {@code attrs}. */
    public Feature copyWithExtraAttrs(Map<String, Object> extraAttrs) {
      return new Feature(
        layer,
        id,
        geometry,
        Stream.concat(attrs.entrySet().stream(), extraAttrs.entrySet().stream())
          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)),
        group
      );
    }
  }

  /**
   * Encodes a geometry as a sequence of integers according to the
   * <a href="https://github.com/mapbox/vector-tile-spec/tree/master/2.1#43-geometry-encoding">Geometry * Encoding
   * Specification</a>.
   */
  private static class CommandEncoder {

    final IntArrayList result = new IntArrayList();
    private final double SCALE;
    // Initial points use absolute locations, then subsequent points in a geometry use offsets so
    // need to keep track of previous x/y location during the encoding.
    int x = 0, y = 0;

    CommandEncoder(int scale) {
      this.SCALE = (EXTENT << scale) / SIZE;
    }

    static boolean shouldClosePath(Geometry geometry) {
      return (geometry instanceof Polygon) || (geometry instanceof LinearRing);
    }

    static int commandAndLength(Command command, int repeat) {
      return repeat << 3 | command.value;
    }

    void accept(Geometry geometry) {
      switch (geometry) {
        case MultiLineString multiLineString -> {
          for (int i = 0; i < multiLineString.getNumGeometries(); i++) {
            encode(((LineString) multiLineString.getGeometryN(i)).getCoordinateSequence(), false, GeometryType.LINE);
          }
        }
        case Polygon polygon -> {
          LineString exteriorRing = polygon.getExteriorRing();
          encode(exteriorRing.getCoordinateSequence(), true, GeometryType.POLYGON);
          for (int i = 0; i < polygon.getNumInteriorRing(); i++) {
            LineString interiorRing = polygon.getInteriorRingN(i);
            encode(interiorRing.getCoordinateSequence(), true, GeometryType.LINE);
          }
        }
        case MultiPolygon multiPolygon -> {
          for (int i = 0; i < multiPolygon.getNumGeometries(); i++) {
            accept(multiPolygon.getGeometryN(i));
          }
        }
        case LineString lineString ->
          encode(lineString.getCoordinateSequence(), shouldClosePath(geometry), GeometryType.LINE);
        case Point point -> encode(point.getCoordinateSequence(), false, GeometryType.POINT);
        case Puntal ignored -> encode(new CoordinateArraySequence(geometry.getCoordinates()), shouldClosePath(geometry),
          geometry instanceof MultiPoint, GeometryType.POINT);
        case null -> LOGGER.warn("Null geometry type");
        default -> LOGGER.warn("Unrecognized geometry type: " + geometry.getGeometryType());
      }
    }

    void encode(CoordinateSequence cs, boolean closePathAtEnd, GeometryType geomType) {
      encode(cs, closePathAtEnd, false, geomType);
    }

    void encode(CoordinateSequence cs, boolean closePathAtEnd, boolean multiPoint, GeometryType geomType) {
      if (cs.size() == 0) {
        throw new IllegalArgumentException("empty geometry");
      }

      int startIdx = result.size();
      int numPoints = 0;
      int lineToIndex = 0;
      int lineToLength = 0;
      int startX = x;
      int startY = y;

      for (int i = 0; i < cs.size(); i++) {

        double cx = cs.getX(i);
        double cy = cs.getY(i);

        if (i == 0) {
          result.add(commandAndLength(Command.MOVE_TO, multiPoint ? cs.size() : 1));
        }

        int _x = (int) Math.round(cx * SCALE);
        int _y = (int) Math.round(cy * SCALE);

        // prevent point equal to the previous
        if (i > 0 && _x == x && _y == y && !multiPoint) {
          lineToLength--;
          continue;
        }

        // prevent double closing
        if (closePathAtEnd && cs.size() > 1 && i == (cs.size() - 1) && cs.getX(0) == cx && cs.getY(0) == cy) {
          lineToLength--;
          continue;
        }

        // delta, then zigzag
        result.add(zigZagEncode(_x - x));
        result.add(zigZagEncode(_y - y));
        numPoints++;

        x = _x;
        y = _y;

        if (i == 0 && cs.size() > 1 && !multiPoint) {
          // can length be too long?
          lineToIndex = result.size();
          lineToLength = cs.size() - 1;
          result.add(commandAndLength(Command.LINE_TO, lineToLength));
        }

      }

      // update LineTo length
      if (lineToIndex > 0) {
        if (lineToLength == 0) {
          // remove empty LineTo
          result.remove(lineToIndex);
        } else {
          // update LineTo with new length
          result.set(lineToIndex, commandAndLength(Command.LINE_TO, lineToLength));
        }
      }

      if (closePathAtEnd) {
        result.add(commandAndLength(Command.CLOSE_PATH, 1));
        numPoints++;
      }

      // degenerate geometry, skip emitting
      if (numPoints < geomType.minPoints()) {
        result.elementsCount = startIdx;
        // reset deltas
        x = startX;
        y = startY;
      }
    }
  }

  private record EncodedFeature(IntArrayList tags, long id, VectorGeometry geometry) {

    EncodedFeature(Feature in) {
      this(new IntArrayList(), in.id(), in.geometry());
    }
  }

  /**
   * Holds all features in an output layer of this tile, along with the index of each tag key/value so that features can
   * store each key/value as a pair of integers.
   */
  private static final class Layer {

    final List<EncodedFeature> encodedFeatures = new ArrayList<>();
    final Map<String, Integer> keys = new LinkedHashMap<>();
    final Map<Object, Integer> values = new LinkedHashMap<>();

    List<String> keys() {
      return new ArrayList<>(keys.keySet());
    }

    List<Object> values() {
      return new ArrayList<>(values.keySet());
    }

    /** Returns the ID associated with {@code key} or adds a new one if not present. */
    Integer key(String key) {
      Integer i = keys.get(key);
      if (i == null) {
        i = keys.size();
        keys.put(key, i);
      }
      return i;
    }

    /** Returns the ID associated with {@code value} or adds a new one if not present. */
    Integer value(Object value) {
      Integer i = values.get(value);
      if (i == null) {
        i = values.size();
        values.put(value, i);
      }
      return i;
    }

    @Override
    public String toString() {
      return "Layer{" + encodedFeatures.size() + "}";
    }
  }
}
