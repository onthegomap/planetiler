/*****************************************************************
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
package com.onthegomap.flatmap;

import com.carrotsearch.hppc.DoubleArrayList;
import com.carrotsearch.hppc.IntArrayList;
import com.google.common.primitives.Ints;
import com.google.protobuf.InvalidProtocolBufferException;
import com.onthegomap.flatmap.geo.GeoUtils;
import com.onthegomap.flatmap.geo.GeometryException;
import com.onthegomap.flatmap.geo.TileCoord;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.locationtech.jts.algorithm.Orientation;
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
import vector_tile.VectorTile;
import vector_tile.VectorTile.Tile.GeomType;

/**
 * This class is copied from https://github.com/ElectronicChartCentre/java-vector-tile/blob/master/src/main/java/no/ecc/vectortile/VectorTileEncoder.java
 * and https://github.com/ElectronicChartCentre/java-vector-tile/blob/master/src/main/java/no/ecc/vectortile/VectorTileDecoder.java
 * and modified.
 * <p>
 * The modifications decouple geometry encoding from vector tile encoding so that encoded commands can be stored in the
 * sorted feature map prior to encoding vector tiles.  The internals are also refactored to improve performance by using
 * hppc primitive collections.
 */
public class VectorTileEncoder {

  private static final Logger LOGGER = LoggerFactory.getLogger(VectorTileEncoder.class);

  private static final int EXTENT = 4096;
  private static final double SIZE = 256d;
  private static final double SCALE = ((double) EXTENT) / SIZE;
  private final Map<String, Layer> layers = new LinkedHashMap<>();

  private static int[] getCommands(Geometry input) {
    var encoder = new CommandEncoder();
    encoder.accept(input);
    return encoder.result.toArray();
  }

  private static VectorTile.Tile.GeomType toGeomType(Geometry geometry) {
    if (geometry instanceof Point || geometry instanceof MultiPoint) {
      return VectorTile.Tile.GeomType.POINT;
    } else if (geometry instanceof LineString || geometry instanceof MultiLineString) {
      return VectorTile.Tile.GeomType.LINESTRING;
    } else if (geometry instanceof Polygon || geometry instanceof MultiPolygon) {
      return VectorTile.Tile.GeomType.POLYGON;
    }
    return VectorTile.Tile.GeomType.UNKNOWN;
  }

  private static CoordinateSequence toCs(DoubleArrayList seq) {
    return new PackedCoordinateSequence.Double(seq.toArray(), 2, 0);
  }

  private static int zigZagEncode(int n) {
    // https://developers.google.com/protocol-buffers/docs/encoding#types
    return (n << 1) ^ (n >> 31);
  }

  private static int zigZagDecode(int n) {
    // https://developers.google.com/protocol-buffers/docs/encoding#types
    return ((n >> 1) ^ (-(n & 1)));
  }

  private static Geometry decodeCommands(byte geomTypeByte, int[] commands) throws GeometryException {
    try {
      VectorTile.Tile.GeomType geomType = Objects.requireNonNull(VectorTile.Tile.GeomType.forNumber(geomTypeByte));
      GeometryFactory gf = GeoUtils.JTS_FACTORY;
      int x = 0;
      int y = 0;

      List<DoubleArrayList> coordsList = new ArrayList<>();
      DoubleArrayList coords = null;

      int geometryCount = commands.length;
      int length = 0;
      int command = 0;
      int i = 0;
      while (i < geometryCount) {

        if (length <= 0) {
          length = commands[i++];
          command = length & ((1 << 3) - 1);
          length = length >> 3;
        }

        if (length > 0) {

          if (command == Command.MOVE_TO.value) {
            coords = new DoubleArrayList();
            coordsList.add(coords);
          } else {
            Objects.requireNonNull(coords);
          }

          if (command == Command.CLOSE_PATH.value) {
            if (geomType != VectorTile.Tile.GeomType.POINT && !coords.isEmpty()) {
              coords.add(coords.get(0), coords.get(1));
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

          coords.add(x / SCALE, y / SCALE);
        }

      }

      Geometry geometry = null;
      boolean outerCCW = false;

      switch (geomType) {
        case LINESTRING:
          List<LineString> lineStrings = new ArrayList<>(coordsList.size());
          for (DoubleArrayList cs : coordsList) {
            if (cs.size() <= 2) {
              continue;
            }
            lineStrings.add(gf.createLineString(toCs(cs)));
          }
          if (lineStrings.size() == 1) {
            geometry = lineStrings.get(0);
          } else if (lineStrings.size() > 1) {
            geometry = gf.createMultiLineString(lineStrings.toArray(new LineString[0]));
          }
          break;
        case POINT:
          CoordinateSequence cs = new PackedCoordinateSequence.Double(coordsList.size(), 2, 0);
          for (int j = 0; j < coordsList.size(); j++) {
            cs.setOrdinate(j, 0, coordsList.get(j).get(0));
            cs.setOrdinate(j, 1, coordsList.get(j).get(1));
          }
          if (cs.size() == 1) {
            geometry = gf.createPoint(cs);
          } else if (cs.size() > 1) {
            geometry = gf.createMultiPoint(cs);
          }
          break;
        case POLYGON:
          List<List<LinearRing>> polygonRings = new ArrayList<>();
          List<LinearRing> ringsForCurrentPolygon = new ArrayList<>();
          boolean first = true;
          for (DoubleArrayList clist : coordsList) {
            // skip hole with too few coordinates
            if (ringsForCurrentPolygon.size() > 0 && clist.size() < 4) {
              continue;
            }
            LinearRing ring = gf.createLinearRing(toCs(clist));
            boolean ccw = Orientation.isCCW(ring.getCoordinates());
            if (first) {
              first = false;
              outerCCW = ccw;
              assert outerCCW;
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
          break;
        default:
          break;
      }

      if (geometry == null) {
        geometry = gf.createGeometryCollection(new Geometry[0]);
      }

      return geometry;
    } catch (IllegalArgumentException e) {
      throw new GeometryException("Unable to decode geometry", e);
    }
  }

  public static List<Feature> decode(byte[] encoded) {
    return decode(TileCoord.ofXYZ(0, 0, 0), encoded);
  }

  public static List<Feature> decode(TileCoord tileID, byte[] encoded) {
    try {
      VectorTile.Tile tile = VectorTile.Tile.parseFrom(encoded);
      List<Feature> features = new ArrayList<>();
      for (VectorTile.Tile.Layer layer : tile.getLayersList()) {
        String layerName = layer.getName();
        assert layer.getExtent() == 4096;
        List<String> keys = layer.getKeysList();
        List<Object> values = new ArrayList<>();

        for (VectorTile.Tile.Value value : layer.getValuesList()) {
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

        for (VectorTile.Tile.Feature feature : layer.getFeaturesList()) {
          int tagsCount = feature.getTagsCount();
          Map<String, Object> attrs = new HashMap<>(tagsCount / 2);
          int tagIdx = 0;
          while (tagIdx < feature.getTagsCount()) {
            String key = keys.get(feature.getTags(tagIdx++));
            Object value = values.get(feature.getTags(tagIdx++));
            attrs.put(key, value);
          }
          try {
            Geometry geometry = decodeCommands(feature.getType(), feature.getGeometryList());
            features.add(new Feature(
              layerName,
              feature.getId(),
              encodeGeometry(geometry),
              attrs
            ));
          } catch (GeometryException e) {
            LOGGER.warn("Error decoding " + tileID + ": " + e);
          }
        }
      }
      return features;
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalStateException(e);
    }
  }

  private static Geometry decodeCommands(GeomType type, List<Integer> geometryList) throws GeometryException {
    return decodeCommands((byte) type.getNumber(), geometryList.stream().mapToInt(i -> i).toArray());
  }

  public static VectorGeometry encodeGeometry(Geometry geometry) {
    return new VectorGeometry(getCommands(geometry), (byte) toGeomType(geometry).getNumber());
  }

  public VectorTileEncoder addLayerFeatures(String layerName, List<? extends Feature> features) {
    if (features.isEmpty()) {
      return this;
    }

    Layer layer = layers.get(layerName);
    if (layer == null) {
      layer = new Layer();
      layers.put(layerName, layer);
    }

    for (Feature inFeature : features) {
      if (inFeature.geometry().commands().length > 0) {
        EncodedFeature outFeature = new EncodedFeature(inFeature);

        for (Map.Entry<String, ?> e : inFeature.attrs().entrySet()) {
          // skip attribute without value
          if (e.getValue() == null) {
            continue;
          }
          outFeature.tags.add(layer.key(e.getKey()));
          outFeature.tags.add(layer.value(e.getValue()));
        }

        layer.encodedFeatures.add(outFeature);
      }
    }
    return this;
  }

  public byte[] encode() {
    VectorTile.Tile.Builder tile = VectorTile.Tile.newBuilder();
    for (Map.Entry<String, Layer> e : layers.entrySet()) {
      String layerName = e.getKey();
      Layer layer = e.getValue();

      VectorTile.Tile.Layer.Builder tileLayer = VectorTile.Tile.Layer.newBuilder();

      tileLayer.setVersion(2);
      tileLayer.setName(layerName);

      tileLayer.addAllKeys(layer.keys());

      for (Object value : layer.values()) {
        VectorTile.Tile.Value.Builder tileValue = VectorTile.Tile.Value.newBuilder();
        if (value instanceof String stringValue) {
          tileValue.setStringValue(stringValue);
        } else if (value instanceof Integer intValue) {
          tileValue.setSintValue(intValue);
        } else if (value instanceof Long longValue) {
          tileValue.setSintValue(longValue);
        } else if (value instanceof Float floatValue) {
          tileValue.setFloatValue(floatValue);
        } else if (value instanceof Double doubleValue) {
          tileValue.setDoubleValue(doubleValue);
        } else if (value instanceof Boolean booleanValue) {
          tileValue.setBoolValue(booleanValue);
        } else {
          tileValue.setStringValue(value.toString());
        }
        tileLayer.addValues(tileValue.build());
      }

      tileLayer.setExtent(EXTENT);

      for (EncodedFeature feature : layer.encodedFeatures) {

        VectorTile.Tile.Feature.Builder featureBuilder = VectorTile.Tile.Feature.newBuilder();

        featureBuilder.addAllTags(Ints.asList(feature.tags.toArray()));
        if (feature.id >= 0) {
          featureBuilder.setId(feature.id);
        }

        featureBuilder.setType(VectorTile.Tile.GeomType.forNumber(feature.geometry().geomType()));
        featureBuilder.addAllGeometry(Ints.asList(feature.geometry().commands()));
        tileLayer.addFeatures(featureBuilder.build());
      }

      tile.addLayers(tileLayer.build());
    }
    return tile.build().toByteArray();
  }

  private enum Command {
    MOVE_TO(1),
    LINE_TO(2),
    CLOSE_PATH(7);
    final int value;

    Command(int value) {
      this.value = value;
    }
  }

  public static record VectorGeometry(int[] commands, byte geomType) {

    public Geometry decode() throws GeometryException {
      return decodeCommands(geomType, commands);
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
      result = 31 * result + (int) geomType;
      return result;
    }

    @Override
    public String toString() {
      return "VectorGeometry[" +
        "commands=int[" + commands.length +
        "], geomType=" + geomType +
        " (" + GeomType.forNumber(geomType) +
        ")]";
    }
  }

  public static record Feature(
    String layer,
    long id,
    VectorGeometry geometry,
    Map<String, Object> attrs
  ) {

    public Feature copyWithNewGeometry(Geometry newGeometry) {
      return new Feature(
        layer,
        id,
        encodeGeometry(newGeometry),
        attrs
      );
    }
  }

  private static class CommandEncoder {

    private final IntArrayList result = new IntArrayList();
    private int x = 0, y = 0;

    private static boolean shouldClosePath(Geometry geometry) {
      return (geometry instanceof Polygon) || (geometry instanceof LinearRing);
    }

    private static int commandAndLength(Command command, int repeat) {
      return repeat << 3 | command.value;
    }

    private void accept(Geometry geometry) {
      if (geometry instanceof MultiLineString multiLineString) {
        for (int i = 0; i < multiLineString.getNumGeometries(); i++) {
          encode(((LineString) multiLineString.getGeometryN(i)).getCoordinateSequence(), false);
        }
      } else if (geometry instanceof Polygon polygon) {
        LineString exteriorRing = polygon.getExteriorRing();
        encode(exteriorRing.getCoordinateSequence(), true);

        for (int i = 0; i < polygon.getNumInteriorRing(); i++) {
          LineString interiorRing = polygon.getInteriorRingN(i);
          encode(interiorRing.getCoordinateSequence(), true);
        }
      } else if (geometry instanceof MultiPolygon multiPolygon) {
        for (int i = 0; i < multiPolygon.getNumGeometries(); i++) {
          accept(multiPolygon.getGeometryN(i));
        }
      } else if (geometry instanceof LineString lineString) {
        encode(lineString.getCoordinateSequence(), shouldClosePath(geometry));
      } else if (geometry instanceof Point point) {
        encode(point.getCoordinateSequence(), false);
      } else if (geometry instanceof Puntal) {
        encode(new CoordinateArraySequence(geometry.getCoordinates()), shouldClosePath(geometry),
          geometry instanceof MultiPoint);
      } else {
        LOGGER.warn("Unrecognized geometry type: " + geometry.getGeometryType());
      }
    }

    private void encode(CoordinateSequence cs, boolean closePathAtEnd) {
      encode(cs, closePathAtEnd, false);
    }

    private void encode(CoordinateSequence cs, boolean closePathAtEnd, boolean multiPoint) {

      if (cs.size() == 0) {
        throw new IllegalArgumentException("empty geometry");
      }

      int lineToIndex = 0;
      int lineToLength = 0;

      for (int i = 0; i < cs.size(); i++) {

        double cx = cs.getX(i);
        double cy = cs.getY(i);

        if (i == 0) {
          result.add(commandAndLength(Command.MOVE_TO, multiPoint ? cs.size() : 1));
        }

        int _x = (int) Math.round(cx * SCALE);
        int _y = (int) Math.round(cy * SCALE);

        // prevent point equal to the previous
        if (i > 0 && _x == x && _y == y) {
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
      }
    }
  }

  private static final record EncodedFeature(IntArrayList tags, long id, VectorGeometry geometry) {

    EncodedFeature(Feature in) {
      this(new IntArrayList(), in.id(), in.geometry());
    }
  }

  private static final class Layer {

    private final List<EncodedFeature> encodedFeatures = new ArrayList<>();
    private final Map<String, Integer> keys = new LinkedHashMap<>();
    private final Map<Object, Integer> values = new LinkedHashMap<>();

    public Integer key(String key) {
      Integer i = keys.get(key);
      if (i == null) {
        i = keys.size();
        keys.put(key, i);
      }
      return i;
    }

    public List<String> keys() {
      return new ArrayList<>(keys.keySet());
    }

    public List<Object> values() {
      return new ArrayList<>(values.keySet());
    }

    public Integer value(Object value) {
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
