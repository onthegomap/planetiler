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
import com.onthegomap.planetiler.geo.PolygonIndex;
import com.onthegomap.planetiler.stats.DefaultStats;
import com.onthegomap.planetiler.stats.Stats;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
 * 一个用于在 {@link Profile#postProcessLayerFeatures(String, int, List)} 中合并具有相同属性的特征的工具集合，
 * 在将一个图块写入输出存档之前调用。
 * <p>
 * 与基于 PostGIS 的解决方案不同，后者在将所有特征加载到数据库后可以全面查看所有特征，
 * 而 planetiler 引擎在处理源特征时一次只能看到一个输入特征，
 * 然后在发出之前只在图块内可见多个特征。这对于大多数实际应用场景来说已经足够，
 * 但是如果需要查看多个特征（<em>不</em> 在同一个图块中），
 * {@link Profile} 实现必须手动存储输入特征。
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

  /** 不实例化 */
  private FeatureMerge() {}

  /**
   * 将具有相同属性的线串合并为多线串，其中端点相接的段由 {@link LineMerger} 合并，移除小于 {@code minLength} 的线串。
   * <p>
   * 忽略任何非线串并将其原样传递到输出。
   * <p>
   * 将合并后的多线串按输入列表中第一个元素的索引排序。
   *
   * @param features   图层中的所有特征
   * @param minLength  要发出的特征的最小像素长度，或 0 以发出所有合并的线串
   * @param tolerance  合并后，使用此像素容差简化线串，或 -1 以跳过简化步骤
   * @param buffer     包含详细信息的可见图块区域外的像素数，或 -1 以跳过剪裁步骤
   * @param resimplify 如果线串即使没有与另一个合并也应该简化，则为真
   * @return 一个新列表，其中包含所有未更改的特征（按原始顺序），然后是每个合并组（按输入列表中第一个元素的索引排序）。
   */
  public static List<VectorTile.Feature> mergeLineStrings(List<VectorTile.Feature> features,
    double minLength, double tolerance, double buffer, boolean resimplify) {
    return mergeLineStrings(features, attrs -> minLength, tolerance, buffer, resimplify);
  }

  /**
   * 合并具有相同属性的线串，与 {@link #mergeLineStrings(List, double, double, double, boolean)} 类似，
   * 但默认设置 {@code resimplify=false}。
   */
  public static List<VectorTile.Feature> mergeLineStrings(List<VectorTile.Feature> features,
    double minLength, double tolerance, double buffer) {
    return mergeLineStrings(features, minLength, tolerance, buffer, false);
  }

  /** 将具有相同属性的点合并为多点。 */
  public static List<VectorTile.Feature> mergeMultiPoint(List<VectorTile.Feature> features) {
    return mergeGeometries(features, GeometryType.POINT);
  }

  /**
   * 将具有相同属性的多边形合并为多边形。
   * <p>
   * 注意：这不会尝试合并重叠的几何图形，参见 {@link #mergeOverlappingPolygons(List, double)} 或 {@link #mergeNearbyPolygons(List, double, double, double, double)}。
   */
  public static List<VectorTile.Feature> mergeMultiPolygon(List<VectorTile.Feature> features) {
    return mergeGeometries(features, GeometryType.POLYGON);
  }

  /**
   * 将具有相同属性的线串合并为多线串。
   * <p>
   * 注意：这不会尝试连接端点相交的线串，参见 {@link #mergeLineStrings(List, double, double, double, boolean)}。
   * 此外，这会移除保留的额外详细信息，以提高连接线串的合并性能，因此你应该只使用其中之一。
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
   * 合并具有相同属性的线串，与 {@link #mergeLineStrings(List, Function, double, double, boolean)} 类似，
   * 但默认设置 {@code resimplify=false}。
   */
  public static List<VectorTile.Feature> mergeLineStrings(List<VectorTile.Feature> features,
    Function<Map<String, Object>, Double> lengthLimitCalculator, double tolerance, double buffer) {
    return mergeLineStrings(features, lengthLimitCalculator, tolerance, buffer, false);
  }

  /**
   * 合并具有相同属性的线串，与 {@link #mergeLineStrings(List, double, double, double, boolean)} 类似，
   * 但使用 {@code lengthLimitCalculator} 动态计算每组的长度限制。
   */
  public static List<VectorTile.Feature> mergeLineStrings(List<VectorTile.Feature> features,
    Function<Map<String, Object>, Double> lengthLimitCalculator, double tolerance, double buffer, boolean resimplify) {
    List<VectorTile.Feature> result = new ArrayList<>(features.size());
    var groupedByAttrs = groupByAttrs(features, result, GeometryType.LINE);
    for (List<VectorTile.Feature> groupedFeatures : groupedByAttrs) {
      VectorTile.Feature feature1 = groupedFeatures.getFirst();
      double lengthLimit = lengthLimitCalculator.apply(feature1.tags());

      // 作为一种快捷方式，可以跳过线合并，只有当：
      // - 组中只有 1 个元素
      // - 不需要剪裁
      // - 它不能因太短而被过滤掉
      // - 它不需要简化
      if (groupedFeatures.size() == 1 && buffer == 0d && lengthLimit == 0 && (!resimplify || tolerance == 0)) {
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
            // 重新简化，因为合并段的某些端点可能不必要
            if (line.getNumPoints() > 2 && tolerance >= 0) {
              Geometry simplified = DouglasPeuckerSimplifier.simplify(line, tolerance);
              if (simplified instanceof LineString simpleLineString) {
                line = simpleLineString;
              } else {
                LOGGER.warn("line string merge simplify emitted {}", simplified.getGeometryType());
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
          outputSegments = sortByHilbertIndex(outputSegments);
          Geometry newGeometry = GeoUtils.combineLineStrings(outputSegments);
          result.add(feature1.copyWithNewGeometry(newGeometry));
        }
      }
    }
    return result;
  }

  /**
   * 从 {@code input} 中移除起点和终点都在图块边界（加上 {@code buffer}）外的段，并将结果段放入 {@code output} 中。
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

    // 最后一个点
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
   * 将具有相同属性的多边形合并为覆盖相同区域的多边形，重叠/相接的多边形合并为更少的多边形。
   * <p>
   * 忽略任何非多边形并将其原样传递到输出。
   * <p>
   * 将合并后的多边形按输入列表中第一个元素的索引排序。
   *
   * @param features 图层中的所有特征
   * @param minArea  要发出的多边形的最小像素面积
   * @return 一个新列表，其中包含所有未更改的特征（按原始顺序），然后是每个合并组（按输入列表中第一个元素的索引排序）。
   * @throws GeometryException 如果编码合并的几何图形时发生错误
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
   * 合并在 {@code minDist} 范围内的具有相同属性的多边形，通过扩展然后收缩合并的几何图形 {@code buffer} 来合并几乎相接的多边形。
   * <p>
   * 忽略任何非多边形并将其原样传递到输出。
   * <p>
   * 将合并后的多边形按输入列表中第一个元素的索引排序。
   *
   * @param features    图层中的所有特征
   * @param minArea     要发出的多边形的最小像素面积
   * @param minHoleArea 多边形内环的最小像素面积
   * @param minDist     多边形之间的最小距离阈值，以像素为单位，合并为一组
   * @param buffer      扩展然后收缩多边形以合并几乎相接的多边形的数量（以像素为单位）
   * @param stats       用于统计数据错误
   * @return 一个新列表，其中包含所有未更改的特征（按原始顺序），然后是每个合并组（按输入列表中第一个元素的索引排序）。
   * @throws GeometryException 如果编码合并的几何图形时发生错误
   */
  public static List<VectorTile.Feature> mergeNearbyPolygons(List<VectorTile.Feature> features, double minArea,
    double minHoleArea, double minDist, double buffer, Stats stats) throws GeometryException {
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
            // 有两种合并多边形的方法：
            // 1) bufferUnbuffer: merged.buffer(amount).buffer(-amount)
            // 2) bufferUnionUnbuffer: 对每个多边形执行 polygon.buffer(amount)，然后合并 merged.union().buffer(-amount)
            // 方法 #1 在大多数情况下较快，但当缓冲多边形之间存在大重叠时（即大多数小于缓冲量）会变得非常慢并占用大量内存，因此我们使用 #2 避免在非常密集的图块上长时间运行。
            // TODO 使用某种启发式方法，根据组中小多边形的数量选择 bufferUnbuffer 或 bufferUnionUnbuffer？
            merged = bufferUnionUnbuffer(buffer, polygonGroup, stats);
          } else {
            merged = buffer(buffer, GeoUtils.createGeometryCollection(polygonGroup));
          }
          if (!(merged instanceof Polygonal) || merged.getEnvelopeInternal().getArea() < minArea) {
            continue;
          }
          merged = GeoUtils.snapAndFixPolygon(merged, stats, "merge").reverse();
        } else {
          merged = polygonGroup.getFirst();
          if (!(merged instanceof Polygonal) || merged.getEnvelopeInternal().getArea() < minArea) {
            continue;
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
    return mergeNearbyPolygons(features, minArea, minHoleArea, minDist, buffer, DefaultStats.get());
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
   * 返回具有相同属性的矢量图块特征组。
   *
   * @param features     输入特征集
   * @param others       添加不匹配 {@code geometryType} 的任何特征的列表
   * @param geometryType 返回结果中的几何类型
   * @return 所有类型为 {@code geometryType} 的特征，按属性分组
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
   * 通过扩展每个单独的多边形 {@code buffer}，将它们合并，然后收缩结果。
   */
  static Geometry bufferUnionUnbuffer(double buffer, List<Geometry> polygonGroup, Stats stats) {
    /*
     * 另一个看起来更简单更快的替代方案可能是：
     *
     * Geometry merged = GeoUtils.createGeometryCollection(polygonGroup);
     * merged = buffer(buffer, merged);
     * merged = unbuffer(buffer, merged);
     *
     * 但由于 buffer() 比 union() 快，只有当重叠量较小时，这种技术在合并许多密集的附近多边形时变得非常慢。
     *
     * 以下方法在大多数情况下更慢，但在平均情况下更快，因为它不会在密集的附近多边形上卡住：
     */
    List<Geometry> buffered = new ArrayList<>(polygonGroup.size());
    for (Geometry geometry : polygonGroup) {
      buffered.add(buffer(buffer, geometry));
    }
    Geometry merged = GeoUtils.createGeometryCollection(buffered);
    try {
      merged = union(merged);
    } catch (TopologyException e) {
      // 缓冲结果有时无效，这使得 union 抛出异常，因此修复它并重试（见 #700）
      stats.dataError("buffer_union_unbuffer_union_failed");
      merged = GeometryFixer.fix(merged);
      merged = union(merged);
    }
    merged = unbuffer(buffer, merged);
    return merged;
  }

  // 这些小包装使得使用 jvisualvm 进行性能分析更容易...
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
   * 将 {@code geom} 中大于 {@code minArea} 的所有多边形放入 {@code result}，去除小于 {@code minHoleArea} 的孔。
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

  /** 返回一个从索引到每个在 {@code minDist} 范围内的几何图形的索引的地图。 */
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
    // 进行迭代（而不是递归）深度优先搜索，因为在处理非常密集的区域（如雅加达）的 z13 建筑物时，递归调用可能会产生 stackoverflow 错误
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
            // 从技术上讲，深度优先搜索会按相反顺序推入堆栈，但由于排序无关紧要，因此不需要这样做
            stack.push(index);
          }
        }
      }
    }
  }

  public static List<VectorTile.Feature> mergeSmallFeatures(
    List<VectorTile.Feature> features,
    int zoom,
    double baseArea,
    double baseLength,
    double tolerance,
    double buffer,
    boolean reSimplify
  ) throws GeometryException {
    Stats stats = DefaultStats.get();
    PolygonIndex<VectorTile.Feature> index = PolygonIndex.create();

    for (VectorTile.Feature feature : features) {
      index.put(feature.geometry().decode(), feature);
    }

    List<VectorTile.Feature> filterBigFeature = new ArrayList<>(features);
    Set<VectorTile.Feature> containedFeatures = new HashSet<>();
    for (VectorTile.Feature feature : features) {
      if (containedFeatures.contains(feature)) {
        continue;
      }

      List<VectorTile.Feature> covers = index.getCovers(feature.geometry().decode());
      covers.remove(feature);
      containedFeatures.addAll(covers);
    }
    filterBigFeature.removeAll(containedFeatures);

    double minArea = baseArea / Math.pow(2, zoom);
    double minLength = baseLength / Math.pow(2, zoom);

    List<VectorTile.Feature> result = new ArrayList<>();

    // 处理多边形
    List<VectorTile.Feature> smallPolygons = new ArrayList<>();
    for (VectorTile.Feature feature : filterBigFeature) {
      Geometry geometry = feature.geometry().decode();
      if (geometry instanceof Polygon && geometry.getArea() < 10) {
        smallPolygons.add(feature);
      } else {
        result.add(feature); // 其他要素直接加入结果集
      }
    }

    if (!smallPolygons.isEmpty()) {
      result.addAll(mergeNearbyPolygons(smallPolygons, 10, 0, minLength, buffer, stats));
    }

    return filterBigFeature;
  }

  private static List<VectorTile.Feature> mergeSmallPolygons(
    List<VectorTile.Feature> smallPolygons,
    double minArea,
    double tolerance,
    double buffer,
    boolean resimplify,
    Stats stats
  ) throws GeometryException {

    List<Polygon> outPolygons = new ArrayList<>();
    List<VectorTile.Feature> result = new ArrayList<>();

    for (VectorTile.Feature feature : smallPolygons) {
      Geometry geometry = feature.geometry().decode();
      if (geometry instanceof Polygon polygon) {
        outPolygons.add(polygon);
      }
    }

    if (!outPolygons.isEmpty()) {
      Geometry combinedGeometry = GeoUtils.combinePolygons(outPolygons);

      if (resimplify && tolerance >= 0) {
        combinedGeometry = DouglasPeuckerSimplifier.simplify(combinedGeometry, tolerance);
      }

      if (buffer >= 0) {
        List<Polygon> outputPolygons = new ArrayList<>();
        FeatureMerge.extractPolygons(combinedGeometry, outputPolygons, minArea, 0);
        combinedGeometry = GeoUtils.combinePolygons(outputPolygons);
      }

      result.add(smallPolygons.getFirst().copyWithNewGeometry(combinedGeometry));
    }

    return result;
  }


  /**
   * 返回一个新特征列表，其中删除了距图块边界超过 {@code buffer} 像素的点，假设图块为 256x256 像素。
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
