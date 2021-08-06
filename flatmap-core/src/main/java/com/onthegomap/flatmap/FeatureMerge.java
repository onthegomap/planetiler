package com.onthegomap.flatmap;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.IntStack;
import com.graphhopper.coll.GHIntObjectHashMap;
import com.onthegomap.flatmap.geo.DouglasPeuckerSimplifier;
import com.onthegomap.flatmap.geo.GeoUtils;
import com.onthegomap.flatmap.geo.GeometryException;
import com.onthegomap.flatmap.geo.GeometryType;
import com.onthegomap.flatmap.geo.MutableCoordinateSequence;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.locationtech.jts.algorithm.Area;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.Polygonal;
import org.locationtech.jts.index.strtree.STRtree;
import org.locationtech.jts.operation.buffer.BufferOp;
import org.locationtech.jts.operation.buffer.BufferParameters;
import org.locationtech.jts.operation.linemerge.LineMerger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FeatureMerge {

  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureMerge.class);

  public static List<VectorTileEncoder.Feature> mergeLineStrings(List<VectorTileEncoder.Feature> items,
    double minLength, double tolerance, double clip) throws GeometryException {
    return mergeLineStrings(items, attrs -> minLength, tolerance, clip);
  }

  public static List<VectorTileEncoder.Feature> mergeLineStrings(List<VectorTileEncoder.Feature> features,
    Function<Map<String, Object>, Double> lengthLimitCalculator, double tolerance, double clip)
    throws GeometryException {
    List<VectorTileEncoder.Feature> result = new ArrayList<>(features.size());
    var groupedByAttrs = groupByAttrs(features, result, GeometryType.LINE);
    for (List<VectorTileEncoder.Feature> groupedFeatures : groupedByAttrs) {
      VectorTileEncoder.Feature feature1 = groupedFeatures.get(0);
      double lengthLimit = lengthLimitCalculator.apply(feature1.attrs());

      // as a shortcut, can skip line merging only if there is:
      // - only 1 element in the group
      // - it doesn't need to be clipped
      // - it can't possibly be filtered out for being too short
      if (groupedFeatures.size() == 1 && clip == 0d && lengthLimit == 0) {
        result.add(feature1);
      } else {
        LineMerger merger = new LineMerger();
        for (VectorTileEncoder.Feature feature : groupedFeatures) {
          merger.add(feature.geometry().decode());
        }
        List<LineString> outputSegments = new ArrayList<>();
        for (Object merged : merger.getMergedLineStrings()) {
          if (merged instanceof LineString line && line.getLength() >= lengthLimit) {
            // re-simplify since some endpoints of merged segments may be unnecessary
            if (line.getNumPoints() > 2) {
              Geometry simplified = DouglasPeuckerSimplifier.simplify(line, tolerance);
              if (simplified instanceof LineString simpleLineString) {
                line = simpleLineString;
              } else {
                LOGGER.warn("line string merge simplify emitted " + simplified.getGeometryType());
              }
            }
            if (clip > 0) {
              removeDetailOutsideTile(line, clip, outputSegments);
            } else {
              outputSegments.add(line);
            }
          }
        }
        if (outputSegments.size() == 0) {
          // no segments to output - skip this feature
        } else {
          Geometry newGeometry = GeoUtils.combineLineStrings(outputSegments);
          result.add(feature1.copyWithNewGeometry(newGeometry));
        }
      }
    }
    return result;
  }

  private static void removeDetailOutsideTile(LineString input, double buffer, List<LineString> output) {
    MutableCoordinateSequence current = new MutableCoordinateSequence();
    CoordinateSequence seq = input.getCoordinateSequence();
    boolean wasIn = false;
    double min = -buffer, max = 256 + buffer;
    double x = seq.getX(0), y = seq.getY(0);
    Envelope env = new Envelope();
    Envelope outer = new Envelope(min, max, min, max);
    for (int i = 0; i < seq.size() - 1; i++) {
      double nextX = seq.getX(i + 1), nextY = seq.getY(i + 1);
      env.init(x, nextX, y, nextY);
      boolean nowIn = env.intersects(outer);
      if (nowIn || wasIn) {
        current.addPoint(x, y);
      } else { // out
        // wait to flush until 2 consecutive outs
        if (!current.isEmpty()) {
          if (current.size() >= 2) {
            output.add(GeoUtils.JTS_FACTORY.createLineString(current));
          }
          current = new MutableCoordinateSequence();
        }
      }
      wasIn = nowIn;
      x = nextX;
      y = nextY;
    }
    double lastX = seq.getX(seq.size() - 1), lastY = seq.getY(seq.size() - 1);
    env.init(x, lastX, y, lastY);
    if (env.intersects(outer) || wasIn) {
      current.addPoint(lastX, lastY);
    }

    if (current.size() >= 2) {
      output.add(GeoUtils.JTS_FACTORY.createLineString(current));
    }
  }

  private static final BufferParameters bufferOps = new BufferParameters();

  static {
    bufferOps.setJoinStyle(BufferParameters.JOIN_MITRE);
  }

  public static List<VectorTileEncoder.Feature> mergePolygons(List<VectorTileEncoder.Feature> features, double minArea,
    double minDist, double buffer) throws GeometryException {
    return mergePolygons(
      features,
      minArea,
      0,
      minDist,
      buffer
    );
  }

  public static List<VectorTileEncoder.Feature> mergePolygons(List<VectorTileEncoder.Feature> features, double minArea,
    double minHoleArea, double minDist, double buffer) throws GeometryException {
    List<VectorTileEncoder.Feature> result = new ArrayList<>(features.size());
    Collection<List<VectorTileEncoder.Feature>> groupedByAttrs = groupByAttrs(features, result, GeometryType.POLYGON);
    for (List<VectorTileEncoder.Feature> groupedFeatures : groupedByAttrs) {
      List<Polygon> outPolygons = new ArrayList<>();
      VectorTileEncoder.Feature feature1 = groupedFeatures.get(0);
      List<Geometry> geometries = new ArrayList<>(groupedFeatures.size());
      for (var feature : groupedFeatures) {
        geometries.add(feature.geometry().decode());
      }
      Collection<List<Geometry>> groupedByProximity = groupPolygonsByProximity(geometries, minDist);
      for (List<Geometry> polygonGroup : groupedByProximity) {
        Geometry merged;
        if (polygonGroup.size() > 1) {
          if (buffer > 0) {
            // there are 2 ways to merge polygons:
            // 1) bufferUnbuffer: merged.buffer(amount).buffer(-amount)
            // 2) bufferUnionUnbuffer: polygon.buffer(amount) on each polygon then merged.union().buffer(-amount)
            // #1 is faster on average, but can become very slow and use a lot of memory when there is a large overlap
            // between buffered polygons (ie. most of them are smaller than the buffer amount) so we use #2 to avoid
            // spinning for a very long time on very dense tiles.
            // TODO use some heuristic to choose bufferUnbuffer vs. bufferUnionUnbuffer based on the number small
            //      polygons in the group?
            merged = bufferUnionUnbuffer(buffer, polygonGroup);
          } else {
            merged = buffer(buffer, GeoUtils.createGeometryCollection(polygonGroup));
          }
          if (!(merged instanceof Polygonal) || merged.getEnvelopeInternal().getArea() < minArea) {
            continue;
          }
          merged = GeoUtils.snapAndFixPolygon(merged).reverse();
        } else {
          merged = polygonGroup.get(0);
          if (!(merged instanceof Polygonal) || merged.getEnvelopeInternal().getArea() < minArea) {
            continue;
          }
        }
        extractPolygons(merged, outPolygons, minArea, minHoleArea);
      }
      if (!outPolygons.isEmpty()) {
        Geometry combined = GeoUtils.combinePolygons(outPolygons);
        result.add(feature1.copyWithNewGeometry(combined));
      }
    }
    return result;
  }

  private static Geometry bufferUnionUnbuffer(double buffer, List<Geometry> polygonGroup) {
    for (int i = 0; i < polygonGroup.size(); i++) {
      polygonGroup.set(i, buffer(buffer, polygonGroup.get(i)));
    }
    Geometry merged = GeoUtils.createGeometryCollection(polygonGroup);
    merged = union(merged);
    merged = unbuffer(buffer, merged);
    return merged;
  }

  private static Geometry bufferUnbuffer(double buffer, List<Geometry> polygonGroup) {
    Geometry merged = GeoUtils.createGeometryCollection(polygonGroup);
    merged = buffer(buffer, merged);
    merged = unbuffer(buffer, merged);
    return merged;
  }

  private static Geometry union(Geometry merged) {
    return merged.union();
  }

  private static Geometry unbuffer(double buffer, Geometry merged) {
    return new BufferOp(merged, bufferOps).getResultGeometry(-buffer);
  }

  private static Geometry buffer(double buffer, Geometry merged) {
    return new BufferOp(merged, bufferOps).getResultGeometry(buffer);
  }

  private static void extractPolygons(Geometry geom, List<Polygon> result, double minArea, double minHoleArea) {
    if (geom instanceof Polygon poly) {
      if (Area.ofRing(poly.getExteriorRing().getCoordinateSequence()) > minArea) {
        int innerRings = poly.getNumInteriorRing();
        if (minHoleArea > 0 && innerRings > 0) {
          List<LinearRing> rings = new ArrayList<>(innerRings);
          for (int i = 0; i < innerRings; i++) {
            LinearRing innerRing = poly.getInteriorRingN(i);
            if (Area.ofRing(innerRing.getCoordinateSequence()) > minArea) {
              rings.add(innerRing);
            }
          }
          if (rings.size() != innerRings) {
            poly = GeoUtils.createPolygon(poly.getExteriorRing(), rings);
          }
        }
        result.add(poly);
      }
    } else if (geom instanceof GeometryCollection) {
      for (int i = 0; i < geom.getNumGeometries(); i++) {
        extractPolygons(geom.getGeometryN(i), result, minArea, minHoleArea);
      }
    }
  }

  private static IntObjectMap<IntArrayList> extractAdjacencyList(List<Geometry> geometries, double minDist) {
    STRtree envelopeIndex = new STRtree();
    for (int i = 0; i < geometries.size(); i++) {
      Geometry a = geometries.get(i);
      Envelope env = a.getEnvelopeInternal().copy();
      env.expandBy(minDist);
      envelopeIndex.insert(env, i);
    }
    IntObjectMap<IntArrayList> result = new GHIntObjectHashMap<>();
    for (int _i = 0; _i < geometries.size(); _i++) {
      int i = _i;
      Geometry a = geometries.get(i);
      envelopeIndex.query(a.getEnvelopeInternal(), object -> {
        if (object instanceof Integer j) {
          Geometry b = geometries.get(j);
          if (a.isWithinDistance(b, minDist)) {
            put(result, i, j);
            put(result, j, i);
          }
        }
      });
    }
    return result;
  }

  private static void put(IntObjectMap<IntArrayList> result, int from, int to) {
    IntArrayList ilist = result.get(from);
    if (ilist == null) {
      result.put(from, ilist = new IntArrayList());
    }
    ilist.add(to);
  }

  static List<IntArrayList> extractConnectedComponents(IntObjectMap<IntArrayList> adjacencyList, int numItems) {
    List<IntArrayList> result = new ArrayList<>();
    BitSet visited = new BitSet(numItems);

    for (int i = 0; i < numItems; i++) {
      if (!visited.get(i)) {
        visited.set(i, true);
        IntArrayList group = new IntArrayList();
        group.add(i);
        result.add(group);
        dfs(i, group, adjacencyList, visited);
      }
    }
    return result;
  }

  private static void dfs(int startNode, IntArrayList group, IntObjectMap<IntArrayList> adjacencyList,
    BitSet visited) {
    // do iterate (not recursive) depth-first search since when merging z13 building in very dense areas like Jakarta
    // recursive calls can generate a stackoverflow error
    IntStack stack = new IntStack();
    stack.add(startNode);
    while (!stack.isEmpty()) {
      int start = stack.pop();
      IntArrayList adjacent = adjacencyList.get(start);
      if (adjacent != null) {
        for (var cursor : adjacent) {
          int index = cursor.value;
          if (!visited.get(index)) {
            visited.set(index, true);
            group.add(index);
            // technically, depth-first search would push onto the stack in reverse order but don't bother since
            // ordering doesn't matter
            stack.push(index);
          }
        }
      }
    }
  }

  private static Collection<List<Geometry>> groupPolygonsByProximity(List<Geometry> geometries, double minDist) {
    IntObjectMap<IntArrayList> adjacencyList = extractAdjacencyList(geometries, minDist);
    List<IntArrayList> groups = extractConnectedComponents(adjacencyList, geometries.size());
    return groups.stream().map(ids -> {
      List<Geometry> geomsInGroup = new ArrayList<>(ids.size());
      for (var cursor : ids) {
        geomsInGroup.add(geometries.get(cursor.value));
      }
      return geomsInGroup;
    }).toList();
  }


  private static Collection<List<VectorTileEncoder.Feature>> groupByAttrs(
    List<VectorTileEncoder.Feature> features, List<VectorTileEncoder.Feature> result, GeometryType geometryType) {
    LinkedHashMap<Map<String, Object>, List<VectorTileEncoder.Feature>> groupedByAttrs = new LinkedHashMap<>();
    for (VectorTileEncoder.Feature feature : features) {
      if (feature.geometry().geomType() != geometryType) {
        // just ignore and pass through non-polygon features
        result.add(feature);
      } else {
        groupedByAttrs
          .computeIfAbsent(feature.attrs(), k -> new ArrayList<>())
          .add(feature);
      }
    }
    return groupedByAttrs.values();
  }
}
