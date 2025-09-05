package com.onthegomap.planetiler;

import static com.onthegomap.planetiler.VectorTile.ALL;
import static com.onthegomap.planetiler.VectorTile.VectorGeometry.getSide;
import static com.onthegomap.planetiler.VectorTile.VectorGeometry.segmentCrossesTile;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.IntStack;
import com.onthegomap.planetiler.collection.Hppc;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.geo.GeometryPipeline;
import com.onthegomap.planetiler.geo.GeometryType;
import com.onthegomap.planetiler.geo.MutableCoordinateSequence;
import com.onthegomap.planetiler.stats.DefaultStats;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.util.LoopLineMerger;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Comparator;
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
import org.locationtech.jts.geom.TopologyException;
import org.locationtech.jts.geom.util.GeometryFixer;
import org.locationtech.jts.index.strtree.STRtree;
import org.locationtech.jts.operation.buffer.BufferOp;
import org.locationtech.jts.operation.buffer.BufferParameters;
import org.locationtech.jts.operation.linemerge.LineMerger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A collection of utilities for merging features with the same attributes in a rendered tile from
 * {@link Profile#postProcessLayerFeatures(String, int, List)} immediately before a tile is written to the output
 * archive.
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
  // this is slightly faster than Comparator.comparingInt
  private static final Comparator<WithIndex<?>> BY_HILBERT_INDEX =
    (o1, o2) -> Integer.compare(o1.hilbert, o2.hilbert);

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
   * @param features   all features in a layer
   * @param minLength  minimum tile pixel length of features to emit, or 0 to emit all merged linestrings
   * @param tolerance  after merging, simplify linestrings using this pixel tolerance, or -1 to skip simplification step
   * @param buffer     number of pixels outside the visible tile area to include detail for, or -1 to skip clipping step
   * @param resimplify True if linestrings should be simplified even if they don't get merged with another
   * @return a new list containing all unaltered features in their original order, then each of the merged groups
   *         ordered by the index of the first element in that group from the input list.
   */
  public static List<VectorTile.Feature> mergeLineStrings(List<VectorTile.Feature> features,
    double minLength, double tolerance, double buffer, boolean resimplify) {
    return mergeLineStrings(features, attrs -> minLength, tolerance, buffer, resimplify, null);
  }

  /**
   * Merges linestrings with the same attributes as {@link #mergeLineStrings(List, double, double, double, boolean)}
   * except sets {@code resimplify=false} by default.
   */
  public static List<VectorTile.Feature> mergeLineStrings(List<VectorTile.Feature> features,
    double minLength, double tolerance, double buffer) {
    return mergeLineStrings(features, minLength, tolerance, buffer, false);
  }

  /** Merges points with the same attributes into multipoints. */
  public static List<VectorTile.Feature> mergeMultiPoint(List<VectorTile.Feature> features) {
    return mergeGeometries(features, GeometryType.POINT);
  }

  /**
   * Merges polygons with the same attributes into multipolygons.
   * <p>
   * NOTE: This does not attempt to combine overlapping geometries, see {@link #mergeOverlappingPolygons(List, double)}
   * or {@link #mergeNearbyPolygons(List, double, double, double, double)} for that.
   */
  public static List<VectorTile.Feature> mergeMultiPolygon(List<VectorTile.Feature> features) {
    return mergeGeometries(features, GeometryType.POLYGON);
  }

  /**
   * Merges linestrings with the same attributes into multilinestrings.
   * <p>
   * NOTE: This does not attempt to connect linestrings that intersect at endpoints, see
   * {@link #mergeLineStrings(List, double, double, double, boolean)} for that. Also, this removes extra detail that was
   * preserved to improve connected-linestring merging, so you should only use one or the other.
   */
  public static List<VectorTile.Feature> mergeMultiLineString(List<VectorTile.Feature> features) {
    return mergeGeometries(features, GeometryType.LINE);
  }

  private static List<VectorTile.Feature> mergeGeometries(
    List<VectorTile.Feature> features,
    GeometryType geometryType
  ) {
    List<VectorTile.Feature> result = new ArrayList<>(features.size());
    var groupedByAttrs = groupByAttrs(features, result, geometryType);
    for (List<VectorTile.Feature> groupedFeatures : groupedByAttrs) {
      VectorTile.Feature feature1 = groupedFeatures.getFirst();
      if (groupedFeatures.size() == 1) {
        result.add(feature1);
      } else {
        VectorTile.VectorGeometryMerger combined = VectorTile.newMerger(geometryType);
        groupedFeatures.stream()
          .map(f -> new WithIndex<>(f, f.geometry().hilbertIndex()))
          .sorted(BY_HILBERT_INDEX)
          .map(d -> d.feature.geometry())
          .forEachOrdered(combined);
        result.add(feature1.copyWithNewGeometry(combined.finish()));
      }
    }
    return result;
  }

  /**
   * Merges linestrings with the same attributes as
   * {@link #mergeLineStrings(List, Function, double, double, boolean, GeometryPipeline)} except sets
   * {@code resimplify=false} by default.
   */
  public static List<VectorTile.Feature> mergeLineStrings(List<VectorTile.Feature> features,
    Function<Map<String, Object>, Double> lengthLimitCalculator, double tolerance, double buffer) {
    return mergeLineStrings(features, lengthLimitCalculator, tolerance, buffer, false, null);
  }

  /**
   * Merges linestrings with the same attributes as {@link #mergeLineStrings(List, double, double, double, boolean)}
   * except with a dynamic length limit computed by {@code lengthLimitCalculator} for the attributes of each group.
   */
  public static List<VectorTile.Feature> mergeLineStrings(List<VectorTile.Feature> features,
    Function<Map<String, Object>, Double> lengthLimitCalculator, double tolerance, double buffer, boolean resimplify,
    GeometryPipeline pipeline) {
    List<VectorTile.Feature> result = new ArrayList<>(features.size());
    var groupedByAttrs = groupByAttrs(features, result, GeometryType.LINE);
    for (List<VectorTile.Feature> groupedFeatures : groupedByAttrs) {
      VectorTile.Feature feature1 = groupedFeatures.getFirst();
      double lengthLimit = lengthLimitCalculator.apply(feature1.tags());

      // as a shortcut, can skip line merging only if:
      // - only 1 element in the group
      // - it doesn't need to be clipped
      // - and it can't possibly be filtered out for being too short
      // - and it does not need to be simplified
      if (groupedFeatures.size() == 1 && buffer <= 0d && lengthLimit <= 0 && (!resimplify || tolerance <= 0)) {
        result.add(feature1);
      } else {
        LoopLineMerger merger = new LoopLineMerger()
          .setTolerance(tolerance)
          .setMergeStrokes(true)
          .setMinLength(lengthLimit)
          .setLoopMinLength(lengthLimit)
          .setStubMinLength(Math.min(0.5, lengthLimit))
          .setSegmentTransform(pipeline);
        for (VectorTile.Feature feature : groupedFeatures) {
          try {
            merger.add(feature.geometry().decode());
          } catch (GeometryException e) {
            e.log("Error decoding vector tile feature for line merge: " + feature);
          }
        }
        List<LineString> outputSegments = new ArrayList<>();
        for (var line : merger.getMergedLineStrings()) {
          if (buffer >= 0) {
            removeDetailOutsideTile(line, buffer, outputSegments);
          } else {
            outputSegments.add(line);
          }
        }

        if (!outputSegments.isEmpty()) {
          outputSegments = sortByHilbertIndex(outputSegments);
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
   * Combines polygons with the same set of attributes within {@code minDist} from each other, expanding then
   * contracting the merged geometry by {@code buffer} to combine polygons that are almost touching.
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
   * @param stats       for counting data errors
   * @param pipeline    a transform that should be applied to each merged polygon in tile pixel coordinates where
   *                    {@code 0,0} is the top-left and {@code 256,256} is the bottom-right corner of the tile
   * @return a new list containing all unaltered features in their original order, then each of the merged groups
   *         ordered by the index of the first element in that group from the input list.
   * @throws GeometryException if an error occurs encoding the combined geometry
   */
  public static List<VectorTile.Feature> mergeNearbyPolygons(List<VectorTile.Feature> features, double minArea,
    double minHoleArea, double minDist, double buffer, Stats stats, GeometryPipeline pipeline)
    throws GeometryException {
    List<VectorTile.Feature> result = new ArrayList<>(features.size());
    Collection<List<VectorTile.Feature>> groupedByAttrs = groupByAttrs(features, result, GeometryType.POLYGON);
    for (List<VectorTile.Feature> groupedFeatures : groupedByAttrs) {
      List<Polygon> outPolygons = new ArrayList<>();
      VectorTile.Feature feature1 = groupedFeatures.getFirst();
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
            merged = bufferUnionUnbuffer(buffer, polygonGroup, stats);
          } else {
            merged = buffer(buffer, GeoUtils.createGeometryCollection(polygonGroup));
          }
          if (!(merged instanceof Polygonal) || merged.getEnvelopeInternal().getArea() < minArea) {
            continue;
          }
          if (pipeline != null) {
            merged = pipeline.apply(merged);
            if (!(merged instanceof Polygonal)) {
              continue;
            }
          }
          merged = GeoUtils.snapAndFixPolygon(merged, stats, "merge").reverse();
        } else {
          merged = polygonGroup.getFirst();
          if (!(merged instanceof Polygonal) || merged.getEnvelopeInternal().getArea() < minArea) {
            continue;
          }
          if (pipeline != null) {
            Geometry after = pipeline.apply(merged);
            if (!(after instanceof Polygonal)) {
              continue;
            } else if (after != merged) {
              merged = GeoUtils.snapAndFixPolygon(after, stats, "merge_after_pipeline").reverse();
            }
          }
        }
        extractPolygons(merged, outPolygons, minArea, minHoleArea);
      }
      if (!outPolygons.isEmpty()) {
        outPolygons = sortByHilbertIndex(outPolygons);
        Geometry combined = GeoUtils.combinePolygons(outPolygons);
        result.add(feature1.copyWithNewGeometry(combined));
      }
    }
    return result;
  }

  private static <G extends Geometry> List<G> sortByHilbertIndex(List<G> geometries) {
    return geometries.stream()
      .map(p -> new WithIndex<>(p, VectorTile.hilbertIndex(p)))
      .sorted(BY_HILBERT_INDEX)
      .map(d -> d.feature)
      .toList();
  }

  public static List<VectorTile.Feature> mergeNearbyPolygons(List<VectorTile.Feature> features, double minArea,
    double minHoleArea, double minDist, double buffer) throws GeometryException {
    return mergeNearbyPolygons(features, minArea, minHoleArea, minDist, buffer, DefaultStats.get(), null);
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
          .computeIfAbsent(feature.tags(), k -> new ArrayList<>())
          .add(feature);
      }
    }
    return groupedByAttrs.values();
  }

  /**
   * Merges nearby polygons by expanding each individual polygon by {@code buffer}, unioning them, and contracting the
   * result.
   */
  static Geometry bufferUnionUnbuffer(double buffer, List<Geometry> polygonGroup, Stats stats) {
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
    List<Geometry> buffered = new ArrayList<>(polygonGroup.size());
    for (Geometry geometry : polygonGroup) {
      buffered.add(buffer(buffer, geometry));
    }
    Geometry merged = GeoUtils.createGeometryCollection(buffered);
    try {
      merged = union(merged);
    } catch (TopologyException e) {
      // buffer result is sometimes invalid, which makes union throw so fix
      // it and try again (see #700)
      stats.dataError("buffer_union_unbuffer_union_failed");
      merged = GeometryFixer.fix(merged);
      merged = union(merged);
    }
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
      double outerArea = Area.ofRing(poly.getExteriorRing().getCoordinateSequence());
      if (outerArea > minArea) {
        int innerRings = poly.getNumInteriorRing();
        List<LinearRing> rings = innerRings == 0 ? List.of() : new ArrayList<>(innerRings);
        for (int i = 0; i < innerRings; i++) {
          LinearRing innerRing = poly.getInteriorRingN(i);
          if (minHoleArea <= 0 || Area.ofRing(innerRing.getCoordinateSequence()) > minArea) {
            rings.add(innerRing);
          }
        }
        LinearRing exteriorRing = poly.getExteriorRing();
        /* optimization: when merged polygon fill the entire tile, replace it with a canonical fill geometry to ensure
         * that filled tiles are byte-for-byte equivalent. This allows archives that deduplicate tiles to better compress
         * large filled areas like the ocean. */
        double fillBuffer = isFill(outerArea, exteriorRing);
        if (fillBuffer >= 0) {
          exteriorRing = createFill(fillBuffer);
        }
        if (rings.size() != innerRings || exteriorRing != poly.getExteriorRing()) {
          poly = GeoUtils.createPolygon(exteriorRing, rings);
        }
        result.add(poly);
      }
    } else if (geom instanceof GeometryCollection) {
      for (int i = 0; i < geom.getNumGeometries(); i++) {
        extractPolygons(geom.getGeometryN(i), result, minArea, minHoleArea);
      }
    }
  }

  private static final double NOT_FILL = -1;

  /** If {@ocde exteriorRing} fills the entire tile, return the number of pixels that it overhangs, otherwise -1. */
  private static double isFill(double outerArea, LinearRing exteriorRing) {
    if (outerArea < 256 * 256) {
      return NOT_FILL;
    }
    double proposedBuffer = (Math.sqrt(outerArea) - 256) / 2;
    double min = -(proposedBuffer * 0.9);
    double max = 256 + proposedBuffer * 0.9;
    int visited = 0;
    var cs = exteriorRing.getCoordinateSequence();
    int nextSide = getSide(cs.getX(0), cs.getY(0), min, max);
    for (int i = 0; i < cs.size() - 1; i++) {
      int side = nextSide;
      visited |= side;
      nextSide = getSide(cs.getX(i + 1), cs.getY(i + 1), min, max);
      if (segmentCrossesTile(side, nextSide)) {
        return NOT_FILL;
      }
    }
    return visited == ALL ? proposedBuffer : NOT_FILL;
  }

  private static LinearRing createFill(double buffer) {
    double min = -buffer;
    double max = buffer + 256;
    return GeoUtils.JTS_FACTORY.createLinearRing(GeoUtils.coordinateSequence(
      min, min,
      max, min,
      max, max,
      min, max,
      min, min
    ));
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

  /**
   * Returns a new list of features with points that are more than {@code buffer} pixels outside the tile boundary
   * removed, assuming a 256x256px tile.
   */
  public static List<VectorTile.Feature> removePointsOutsideBuffer(List<VectorTile.Feature> features, double buffer) {
    if (!Double.isFinite(buffer)) {
      return features;
    }
    List<VectorTile.Feature> result = new ArrayList<>(features.size());
    for (var feature : features) {
      var geometry = feature.geometry();
      if (geometry.geomType() == GeometryType.POINT) {
        var newGeometry = geometry.filterPointsOutsideBuffer(buffer);
        if (!newGeometry.isEmpty()) {
          result.add(feature.copyWithNewGeometry(newGeometry));
        }
      } else {
        result.add(feature);
      }
    }
    return result;
  }

  private record WithIndex<T>(T feature, int hilbert) {}
}
