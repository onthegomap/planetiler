package com.onthegomap.planetiler;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.IntStack;
import com.onthegomap.planetiler.collection.Hppc;
import com.onthegomap.planetiler.geo.DouglasPeuckerSimplifier;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.geo.GeometryType;
import com.onthegomap.planetiler.geo.MutableCoordinateSequence;
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

/**
 * A collection of utilities for merging features with the same attributes in a rendered tile from
 * {@link Profile#postProcessLayerFeatures(String, int, List)} immediately before a tile is written to the output
 * mbtiles file.
 * <p>
 * Unlike postgis-based solutions that have a full view of all features after they are loaded into the database, the
 * planetiler engine only sees a single input feature at a time while processing source features, then only has
 * visibility into multiple features when they are grouped into a tile immediately before emitting. This ends up being
 * sufficient for most real-world use-cases but to do anything more that requires a view of multiple features
 * <em>not</em> within the same tile, {@link Profile} implementations must store input features manually.
 */
public class FeatureMerge {

  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureMerge.class);
  private static final BufferParameters bufferOps = new BufferParameters();

  static {
    bufferOps.setJoinStyle(BufferParameters.JOIN_MITRE);
  }

  /** Don't instantiate */
  private FeatureMerge() {}

  /**
   * Combines linestrings with the same set of attributes into a multilinestring where segments with touching endpoints
   * are merged by {@link LineMerger}, removing any linestrings under {@code minLength}.
   * <p>
   * Ignores any non-linestrings and passes them through to the output unaltered.
   * <p>
   * Orders grouped output multilinestring by the index of the first element in that group from the input list.
   *
   * @param features  all features in a layer
   * @param minLength minimum tile pixel length of features to emit, or 0 to emit all merged linestrings
   * @param tolerance after merging, simplify linestrings using this pixel tolerance, or -1 to skip simplification step
   * @param buffer    number of pixels outside the visible tile area to include detail for, or -1 to skip clipping step
   * @return a new list containing all unaltered features in their original order, then each of the merged groups
   *         ordered by the index of the first element in that group from the input list.
   */
  public static List<VectorTile.Feature> mergeLineStrings(List<VectorTile.Feature> features,
    double minLength, double tolerance, double buffer) {
    return mergeLineStrings(features, attrs -> minLength, tolerance, buffer);
  }

  /**
   * Merges linestrings with the same attributes as {@link #mergeLineStrings(List, double, double, double)} except
   * with a dynamic length limit computed by {@code lengthLimitCalculator} for the attributes of each group.
   */
  public static List<VectorTile.Feature> mergeLineStrings(List<VectorTile.Feature> features,
    Function<Map<String, Object>, Double> lengthLimitCalculator, double tolerance, double buffer) {
    List<VectorTile.Feature> result = new ArrayList<>(features.size());
    var groupedByAttrs = groupByAttrs(features, result, GeometryType.LINE);
    for (List<VectorTile.Feature> groupedFeatures : groupedByAttrs) {
      VectorTile.Feature feature1 = groupedFeatures.get(0);
      double lengthLimit = lengthLimitCalculator.apply(feature1.attrs());

      // as a shortcut, can skip line merging only if:
      // - only 1 element in the group
      // - it doesn't need to be clipped
      // - and it can't possibly be filtered out for being too short
      if (groupedFeatures.size() == 1 && buffer == 0d && lengthLimit == 0) {
        result.add(feature1);
      } else {
        LineMerger merger = new LineMerger();
        for (VectorTile.Feature feature : groupedFeatures) {
          try {
            merger.add(feature.geometry().decode());
          } catch (GeometryException e) {
            e.log("Error decoding vector tile feature for line merge: " + feature);
          }
        }
        List<LineString> outputSegments = new ArrayList<>();
        for (Object merged : merger.getMergedLineStrings()) {
          if (merged instanceof LineString line && line.getLength() >= lengthLimit) {
            // re-simplify since some endpoints of merged segments may be unnecessary
            if (line.getNumPoints() > 2 && tolerance >= 0) {
              Geometry simplified = DouglasPeuckerSimplifier.simplify(line, tolerance);
              if (simplified instanceof LineString simpleLineString) {
                line = simpleLineString;
              } else {
                LOGGER.warn("line string merge simplify emitted " + simplified.getGeometryType());
              }
            }
            if (buffer >= 0) {
              removeDetailOutsideTile(line, buffer, outputSegments);
            } else {
              outputSegments.add(line);
            }
          }
        }
        if (!outputSegments.isEmpty()) {
          Geometry newGeometry = GeoUtils.combineLineStrings(outputSegments);
          result.add(feature1.copyWithNewGeometry(newGeometry));
        }
      }
    }
    return result;
  }

  /**
   * Removes any segments from {@code input} where both the start and end are outside the tile boundary (plus {@code
   * buffer}) and puts the resulting segments into {@code output}.
   */
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

    // last point
    double lastX = seq.getX(seq.size() - 1), lastY = seq.getY(seq.size() - 1);
    env.init(x, lastX, y, lastY);
    if (env.intersects(outer) || wasIn) {
      current.addPoint(lastX, lastY);
    }

    if (current.size() >= 2) {
      output.add(GeoUtils.JTS_FACTORY.createLineString(current));
    }
  }

  /**
   * Combines polygons with the same set of attributes into a multipolygon where overlapping/touching polygons are
   * combined into fewer polygons covering the same area.
   * <p>
   * Ignores any non-polygons and passes them through to the output unaltered.
   * <p>
   * Orders grouped output multipolygon by the index of the first element in that group from the input list.
   *
   * @param features all features in a layer
   * @param minArea  minimum area in square tile pixels of polygons to emit
   * @return a new list containing all unaltered features in their original order, then each of the merged groups
   *         ordered by the index of the first element in that group from the input list.
   * @throws GeometryException if an error occurs encoding the combined geometry
   */
  public static List<VectorTile.Feature> mergeOverlappingPolygons(List<VectorTile.Feature> features, double minArea)
    throws GeometryException {
    return mergeNearbyPolygons(
      features,
      minArea,
      0,
      0,
      0
    );
  }

  /**
   * Combines polygons with the same set of attributes within {@code minDist} from each other, expanding then contracting
   * the merged geometry by {@code buffer} to combine polygons that are almost touching.
   * <p>
   * Ignores any non-polygons and passes them through to the output unaltered.
   * <p>
   * Orders grouped output multipolygon by the index of the first element in that group from the input list.
   *
   * @param features    all features in a layer
   * @param minArea     minimum area in square tile pixels of polygons to emit
   * @param minHoleArea the minimum area in square tile pixels of inner rings of polygons to emit
   * @param minDist     the minimum threshold in tile pixels between polygons to combine into a group
   * @param buffer      the amount (in tile pixels) to expand then contract polygons by in order to combine
   *                    almost-touching polygons
   * @return a new list containing all unaltered features in their original order, then each of the merged groups
   *         ordered by the index of the first element in that group from the input list.
   * @throws GeometryException if an error occurs encoding the combined geometry
   */
  public static List<VectorTile.Feature> mergeNearbyPolygons(List<VectorTile.Feature> features, double minArea,
    double minHoleArea, double minDist, double buffer) throws GeometryException {
    List<VectorTile.Feature> result = new ArrayList<>(features.size());
    Collection<List<VectorTile.Feature>> groupedByAttrs = groupByAttrs(features, result, GeometryType.POLYGON);
    for (List<VectorTile.Feature> groupedFeatures : groupedByAttrs) {
      List<Polygon> outPolygons = new ArrayList<>();
      VectorTile.Feature feature1 = groupedFeatures.get(0);
      List<Geometry> geometries = new ArrayList<>(groupedFeatures.size());
      for (var feature : groupedFeatures) {
        try {
          geometries.add(feature.geometry().decode());
        } catch (GeometryException e) {
          e.log("Error decoding vector tile feature for polygon merge: " + feature);
        }
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
            // between buffered polygons (i.e. most of them are smaller than the buffer amount) so we use #2 to avoid
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


  /**
   * Returns all the clusters from {@code geometries} where elements in the group are less than {@code minDist} from
   * another element in the group.
   */
  public static Collection<List<Geometry>> groupPolygonsByProximity(List<Geometry> geometries, double minDist) {
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

  /**
   * Returns each group of vector tile features that share the exact same attributes.
   *
   * @param features     the set of input features
   * @param others       list to add any feature that does not match {@code geometryType}
   * @param geometryType the type of geometries to return in the result
   * @return all the elements from {@code features} of type {@code geometryType} grouped by attributes
   */
  public static Collection<List<VectorTile.Feature>> groupByAttrs(
    List<VectorTile.Feature> features,
    List<VectorTile.Feature> others,
    GeometryType geometryType
  ) {
    LinkedHashMap<Map<String, Object>, List<VectorTile.Feature>> groupedByAttrs = new LinkedHashMap<>();
    for (VectorTile.Feature feature : features) {
      if (feature == null) {
        // ignore
      } else if (feature.geometry().geomType() != geometryType) {
        // just ignore and pass through non-polygon features
        others.add(feature);
      } else {
        groupedByAttrs
          .computeIfAbsent(feature.attrs(), k -> new ArrayList<>())
          .add(feature);
      }
    }
    return groupedByAttrs.values();
  }

  /**
   * Merges nearby polygons by expanding each individual polygon by {@code buffer}, unioning them, and contracting the
   * result.
   */
  private static Geometry bufferUnionUnbuffer(double buffer, List<Geometry> polygonGroup) {
    /*
     * A simpler alternative that might initially appear faster would be:
     *
     * Geometry merged = GeoUtils.createGeometryCollection(polygonGroup);
     * merged = buffer(buffer, merged);
     * merged = unbuffer(buffer, merged);
     *
     * But since buffer() is faster than union() only when the amount of overlap is small,
     * this technique becomes very slow for merging many small nearby polygons.
     *
     * The following approach is slower most of the time, but faster on average because it does
     * not choke on dense nearby polygons:
     */
    for (int i = 0; i < polygonGroup.size(); i++) {
      polygonGroup.set(i, buffer(buffer, polygonGroup.get(i)));
    }
    Geometry merged = GeoUtils.createGeometryCollection(polygonGroup);
    merged = union(merged);
    merged = unbuffer(buffer, merged);
    return merged;
  }

  // these small wrappers make performance profiling with jvisualvm easier...
  private static Geometry union(Geometry merged) {
    return merged.union();
  }

  private static Geometry unbuffer(double buffer, Geometry merged) {
    return new BufferOp(merged, bufferOps).getResultGeometry(-buffer);
  }

  private static Geometry buffer(double buffer, Geometry merged) {
    return new BufferOp(merged, bufferOps).getResultGeometry(buffer);
  }

  /**
   * Puts all polygons within {@code geom} over {@code minArea} into {@code result}, removing holes under {@code
   * minHoleArea}.
   */
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

  /** Returns a map from index in {@code geometries} to index of every other geometry within {@code minDist}. */
  private static IntObjectMap<IntArrayList> extractAdjacencyList(List<Geometry> geometries, double minDist) {
    STRtree envelopeIndex = new STRtree();
    for (int i = 0; i < geometries.size(); i++) {
      Geometry a = geometries.get(i);
      Envelope env = a.getEnvelopeInternal().copy();
      env.expandBy(minDist);
      envelopeIndex.insert(env, i);
    }
    IntObjectMap<IntArrayList> result = Hppc.newIntObjectHashMap();
    for (int _i = 0; _i < geometries.size(); _i++) {
      int i = _i;
      Geometry a = geometries.get(i);
      envelopeIndex.query(a.getEnvelopeInternal(), object -> {
        if (object instanceof Integer j) {
          Geometry b = geometries.get(j);
          if (a.isWithinDistance(b, minDist)) {
            addAdjacencyEntry(result, i, j);
            addAdjacencyEntry(result, j, i);
          }
        }
      });
    }
    return result;
  }

  private static void addAdjacencyEntry(IntObjectMap<IntArrayList> result, int from, int to) {
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
        depthFirstSearch(i, group, adjacencyList, visited);
      }
    }
    return result;
  }

  private static void depthFirstSearch(int startNode, IntArrayList group, IntObjectMap<IntArrayList> adjacencyList,
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
}
