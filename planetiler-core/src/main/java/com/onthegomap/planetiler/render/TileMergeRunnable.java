package com.onthegomap.planetiler.render;

import static org.locationtech.jts.geom.LinearRing.MINIMUM_VALID_SIZE;
import static org.locationtech.jts.operation.union.UnaryUnionOp.union;

import com.onthegomap.planetiler.FeatureMerge;
import com.onthegomap.planetiler.VectorTile;
import com.onthegomap.planetiler.archive.Tile;
import com.onthegomap.planetiler.archive.TileEncodingResult;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.geo.GeometryType;
import com.onthegomap.planetiler.geo.MutableCoordinateSequence;
import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.mbtiles.Mbtiles;
import com.onthegomap.planetiler.stats.DefaultStats;
import com.onthegomap.planetiler.util.CloseableIterator;
import com.onthegomap.planetiler.util.Gzip;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import kotlin.Pair;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.TopologyException;
import org.locationtech.jts.geom.util.AffineTransformation;
import org.locationtech.jts.geom.util.GeometryFixer;
import org.locationtech.jts.index.strtree.STRtree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TileMergeRunnable implements Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(TileMergeRunnable.class);

  private static final int DEFAULT_TILE_SIZE = 256;
  private static final double TILE_SCALE = 0.5d;
  public static final String LINESPACE_AREA = "Linespace_Area";

  private static final int EXTENT = 4096;

  public static final int[] gridSizeArray = new int[]{1024, 256, 128};

  private static final GridEntity[] gridEntities = new GridEntity[gridSizeArray.length];

  private static final int GRID_SIZE = 256;

  private static final double SCALE_GEOMETRY = (double) DEFAULT_TILE_SIZE / EXTENT;

  private static final int EXTENT_HALF = EXTENT / 2;

  private final TileCoord tileCoord;

  private final Mbtiles mbtiles;

  private final Mbtiles.TileWriter writer;

  private final PlanetilerConfig config;

  private final GridEntity gridEntity;

  Map<String, List<GeometryWithTag>> originFeatureInfos = new ConcurrentHashMap<>();

  record GeometryWithTag(
    String layer,
    long id,
    Map<String, Object> tags,
    long group,
    Geometry geometry,
    int size,
    double area,
    String hash
  ) {}

//  private static final Geometry[][] VECTOR_GRID = new Geometry[EXTENT][EXTENT];
//  private static final int GRID_WIDTH = EXTENT / GRID_SIZE;
//
//  static {
//    // 初始化一个网格
//    for (int i = 0; i < gridSizeArray.length; i++) {
//      gridEntities[i] = new GridEntity(gridSizeArray[i]);
//    }
//    List<Geometry> geometryList = new ArrayList<>();
//    GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();
//    for (int i = 0; i < EXTENT; i += GRID_WIDTH) {
//      VECTOR_GRID[i] = new Geometry[EXTENT];
//      for (int j = 0; j < EXTENT; j += GRID_WIDTH) {
//        // 创建GeometryFactory实例
//        // 创建地理点
//        if (GRID_WIDTH == 1) {
//          Coordinate coord = new Coordinate(i, j);
//          Point point = geometryFactory.createPoint(coord);
//          VECTOR_GRID[i][j] = point;
//        } else {
//          MutableCoordinateSequence sequence = new MutableCoordinateSequence();
//          sequence.addPoint(i, j);
//          sequence.addPoint(i, j + GRID_WIDTH);
//          sequence.addPoint(i + GRID_WIDTH, j + GRID_WIDTH);
//          sequence.addPoint(i + GRID_WIDTH, j);
//          sequence.addPoint(i, j);
//          Polygon polygon = geometryFactory.createPolygon(sequence);
//          VECTOR_GRID[i][j] = polygon;
//          geometryList.add(polygon);
//        }
//      }
//    }
////    GeometryCollection gridView = geometryFactory.createGeometryCollection(
////      geometryList.toArray(new Geometry[geometryList.size()]));
////    int a = 0;
//  }

  public TileMergeRunnable(TileCoord tileCoord, Mbtiles mbtiles, Mbtiles.TileWriter writer, PlanetilerConfig config,
    GridEntity gridEntity) {
    this.tileCoord = tileCoord;
    this.mbtiles = mbtiles;
    this.writer = writer;
    this.config = config;
    this.gridEntity = gridEntity;
  }

  public TileCoord getTileCoord() {
    return tileCoord;
  }

  @Override
  public void run() {
    LOGGER.info("Starting tile merge thread {}", tileCoord);
    int x = tileCoord.x();
    int y = tileCoord.y();
    int z = tileCoord.z();
    int currentMaxY = (1 << z) - 1;

    // 计算子瓦片的范围 (XYZ坐标)
    int minChildX = x * 2;
    // + 1 的作用是确保计算出的范围包括了父级瓦片对应的所有子级瓦片，覆盖了父级瓦片的整个高度。 -1因为索引从零开始
    int maxChildX = x * 2 + 1;
    int minChildY = (currentMaxY - y) * 2;  // 翻转Y坐标并计算最小Y
    int maxChildY = (currentMaxY - y) * 2 + 1;  // 翻转Y坐标并计算最大Y
    try (CloseableIterator<Tile> tileIterator = mbtiles.getZoomTiles(z + 1, minChildX, minChildY, maxChildX,
      maxChildY)) {
      // 1.读取要素，拼接要素
      long totalSize = 0;
      while (tileIterator.hasNext()) {
        Tile next = tileIterator.next();
        byte[] bytes = next.bytes();
        totalSize += bytes.length;
        processTile(next, z);
      }
      VectorTile mergedTile = new VectorTile();
      VectorTile reduceTile = new VectorTile();
      long maxSize = config.maxFeatures();
      int from = 0;
      int to = 0;
      for (Map.Entry<String, List<GeometryWithTag>> entry : originFeatureInfos.entrySet()) {
        // ---------------------------V2.0---------------------------------------
        // 2. 要素像素化
        List<GeometryWithTag> geometryWithTags = gridMergeFeatures(entry.getValue(), totalSize, maxSize, z);
        List<VectorTile.Feature> reduced = geometryToFeature(geometryWithTags);
        reduceTile.addLayerFeatures(entry.getKey(), reduced);
        // ---------------------------V1.0---------------------------------------
        // 2. 相同属性要素合并，不会产生空洞，不会产生数据丢失
        // 纯纯融合边界，不丢失任何数据
//        List<GeometryWithTag> merged = mergeSameFeatures(entry.getValue());
//        from += merged.size();
//        // 3. 要素重新排序、合并，目标是减少瓦片大小，根据瓦片大小进行，设置=2MB
//        List<VectorTile.Feature> reduced = mergeNearbyFeatures(merged, ratio);
//        to += reduced.size();
      }

      // 4. 一个瓦片只有一个输出结果
      byte[] encode;
      try {
        encode = reduceTile.encode();
      } catch (IllegalStateException e) {
        // TODO: 定位要素编码错误
        return;
      }
      TileEncodingResult result = new TileEncodingResult(tileCoord, Gzip.gzip(encode),
        OptionalLong.empty());
      LOGGER.info("[merged {}]size={} -> {}, count={} -> {}", tileCoord, totalSize, result.tileData().length, from, to);

      // 4.写入MBTiles
      // TODO: write丢失数据
      writer.write(result);
    } catch (Exception e) {
      LOGGER.error(String.format("处理瓦片 %s 时发生错误", tileCoord), e);
    }
    LOGGER.info("Ending tile merge thread {}", tileCoord);
  }

  /**
   * 处理瓦片进行仿真变换
   *
   * @param tile
   */
  private void processTile(Tile tile, int z) throws IOException, GeometryException {
    List<VectorTile.Feature> features = VectorTile.decode(Gzip.gunzip(tile.bytes()));
    for (VectorTile.Feature feature : features) {
      try {
        // 1、拿到4096x4096要素 16MB(每个像素一个要素，会产生16MB大小)
        Geometry decode = feature.geometry().decode(1);
        // 2、将要素移动新的坐标下
        Geometry transformationGeom = simulationTransformation(decode, tile.coord());
        // 处理数据精度
//        transformationGeom = GeoUtils.snapAndFixPolygon(transformationGeom, DefaultStats.get(), "transform",
//          new PrecisionModel(1d));

        if (!transformationGeom.isEmpty()) {
          double shapeArea = Double.parseDouble(feature.getTag(LINESPACE_AREA).toString());
          try {
            if (feature.hasTag("Shape_Area")) {
              shapeArea = Double.parseDouble(feature.getTag("Shape_Area").toString());
            }
          } catch (NumberFormatException ignore) {
          }
          GeometryWithTag geometryWithTags =
            new GeometryWithTag(feature.layer(), feature.id(), feature.tags(),
              feature.group(),
              transformationGeom, feature.geometry().commands().length, shapeArea, feature.tags().toString());
          originFeatureInfos.computeIfAbsent(feature.layer(), k -> new ArrayList<>())
            .add(geometryWithTags);
          LOGGER.debug("tile:{},currentZoomTiles {}", tile, originFeatureInfos.size());
        }
      } catch (AssertionError e) {
        // TODO: 定位要素解码错误
        LOGGER.error("[processTile] {}", tileCoord, e);
      }
    }
  }

  /**
   * 仿真变换
   */
  private Geometry simulationTransformation(Geometry geometry, TileCoord childTile) throws GeometryException {
    int relativeX = childTile.x() & 1;
    int relativeY = childTile.y() & 1;

    AffineTransformation transform = new AffineTransformation();
    double translateX = relativeX * EXTENT * TILE_SCALE;
    double translateY = relativeY * EXTENT * TILE_SCALE;

    Geometry geom = transform.scale(TILE_SCALE, TILE_SCALE).translate(translateX, translateY).transform(geometry);
    if (geom.isEmpty()) {
      LOGGER.error("仿真变换后要素为空！");
    }

    if (!geom.isValid()) {
      geom = GeoUtils.fixPolygon(geom);
    }

    return geom;
  }

  record GeometryWithIndex(Geometry geometry, int index) {}

  private List<VectorTile.Feature> geometryToFeature(List<GeometryWithTag> list) {
    List<VectorTile.Feature> features = new ArrayList<>();
    AffineTransformation transform = new AffineTransformation();
    transform.scale(SCALE_GEOMETRY, SCALE_GEOMETRY);
    List<GeometryWithIndex> gridGeometryWithIndex = new ArrayList<>();
    for (int i = 0; i < list.size(); i++) {
      GeometryWithTag geometryWithTag = list.get(i);
      Geometry geometry = geometryWithTag.geometry();
      // 存储要素、要素ID
      gridGeometryWithIndex.add(new GeometryWithIndex(geometry, i));
    }
    // 合并同类型要素
    Map<Object, List<GeometryWithIndex>> grouped = gridGeometryWithIndex.stream()
      .collect(Collectors.groupingBy(geometryWithIndex -> list.get(geometryWithIndex.index()).tags()));
    List<GeometryWithIndex> geometries = new ArrayList<>();
    // 按照要素的标签进行合并
    grouped.forEach((o, geometryWithIndices) -> {
      if (geometryWithIndices.isEmpty()) {
        return;
      }
      // TODO: 合并，有可能存在不在一个面的情况
      Geometry geometry = GeoUtils.createGeometryCollection(
        geometryWithIndices.stream().map(GeometryWithIndex::geometry).collect(Collectors.toList()));
      Geometry union = geometry.union();
      if (union instanceof Polygon) {
        geometries.add(new GeometryWithIndex(union, geometryWithIndices.getFirst().index()));
      } else if (union instanceof GeometryCollection collection) {
        for (int i = 0; i < collection.getNumGeometries(); i++) {
          Geometry geometryN = collection.getGeometryN(i);
          geometries.add(new GeometryWithIndex(geometryN, geometryWithIndices.getFirst().index()));
        }
      }
    });

    for (GeometryWithIndex geometryWithIndex : geometries) {
      GeometryWithTag geometryWithTag = list.get(geometryWithIndex.index());
      Geometry geometry = geometryWithIndex.geometry();
      Geometry scaled = transform.transform(geometry);
      try {
        VectorTile.Feature feature = getFeature(scaled, geometryWithTag);
        if (feature != null) {
          features.add(feature);
        }
      } catch (Exception e) {
        LOGGER.error("[geometryToFeature] {}", geometryWithTag, e);
      }
    }
    return features;
  }

  private VectorTile.Feature getFeature(Geometry geometry, GeometryWithTag geometryWithTag)
    throws GeometryException {
    List<List<CoordinateSequence>> geoms = GeometryCoordinateSequences.extractGroups(geometry, 0);
    if (geoms.isEmpty()) {
      return null;
    }
    Geometry geom;
    if (geometry instanceof Polygon) {
      geom = GeometryCoordinateSequences.reassemblePolygons(geoms);
      /*
       * Use the very expensive, but necessary JTS Geometry#buffer(0) trick to repair invalid polygons (with self-
       * intersections) and JTS GeometryPrecisionReducer utility to snap polygon nodes to the vector tile grid
       * without introducing self-intersections.
       *
       * See https://docs.mapbox.com/vector-tiles/specification/#simplification for issues that can arise from naive
       * coordinate rounding.
       */
      geom = GeoUtils.snapAndFixPolygon(geom, DefaultStats.get(), "render");
      // JTS utilities "fix" the geometry to be clockwise outer/CCW inner but vector tiles flip Y coordinate,
      // so we need outer CCW/inner clockwise
      geom = geom.reverse();
    } else {
      geom = GeometryCoordinateSequences.reassembleLineStrings(geoms);
    }
    return new VectorTile.Feature(geometryWithTag.layer, geometryWithTag.id,
      VectorTile.encodeGeometry(geom, 0), geometryWithTag.tags, geometryWithTag.group);
  }

  private final Map<String, Pair<Double, Double>> pixelCache = new ConcurrentHashMap<>();

  private List<GeometryWithTag> gridMergeFeatures(List<GeometryWithTag> list, long totalSize, long maxSize, int z)
    throws GeometryException {
//    if (totalSize <= maxSize) {
//      return list;
//    }
    double ratio = Math.max((double) totalSize / (double) maxSize, 1.0); // 表示需要压缩多少倍数据
    int offset = Math.min((int) ratio, gridSizeArray.length - 1); // 避免超出范围
    STRtree envelopeIndex = new STRtree();
    // 将要素添加到R树中
    // TODO: 要素大小排序后，做个数量限制
    for (int i = 0; i < list.size(); i++) {
      Geometry geometry = list.get(i).geometry();
      Envelope env = geometry.getEnvelopeInternal().copy();
      envelopeIndex.insert(env, i);
    }
    // 保存要素+标签
    List<GeometryWithTag> result = new ArrayList<>();
    int gridWidth = gridEntity.getGridWidth();
    Geometry[][] vectorGrid = gridEntity.getVectorGrid();

    // 计算网格集单个像素的面积大小
    String key = z + ":" + EXTENT;
    Pair<Double, Double> pixelArea = pixelCache.computeIfAbsent(key, k -> {
      double areaPerPixel = GeoUtils.areaPerPixelAtEquator(z, EXTENT);
      return new Pair<>(Math.pow(gridWidth, 2) * areaPerPixel, areaPerPixel);
    });

    for (int i = 0; i < EXTENT; i += gridWidth) {
      for (int j = 0; j < EXTENT; j += gridWidth) {
        // 获取网格
        Geometry geometry = vectorGrid[i][j];
        if (geometry == null) {
          continue;
        }
        // 找到相交的数据
        Set<Integer> set = new HashSet<>();
        envelopeIndex.query(geometry.getEnvelopeInternal(), object -> {
          if (object instanceof Integer x) {
            // 这里使用勾股定理
            if (geometry.isWithinDistance(list.get(x).geometry(), 0)) {
              set.add(x);
            }
          }
        });
        List<Integer> sortArea = set.stream().sorted(Comparator.comparingDouble(x -> list.get(x).area())).toList();
        if (!sortArea.isEmpty()) {
          // 找到真实面积最大的瓦片，当前像素归属到
          Integer geoIndex = sortArea.getLast();
          GeometryWithTag geometryWithTag = list.get(geoIndex);

          // 计算要素与像素的比值
          double areaRatio = geometryWithTag.area / pixelArea.getFirst();
          Geometry gridGeometry;
          if (areaRatio >= config.rasterizeAreaThreshold()) {
            // TODO: 处理边界裁剪 当大于阈值时，像素归属该要素
            gridGeometry = geometry;
          }
          else if (geometryWithTag.area > pixelArea.getSecond()) {
            // TODO 计算包络框面积大小 当小于阈值时，计算要素与像素相交范围，获取相交范围的包络框
            gridGeometry = geometry.intersection(geometryWithTag.geometry).getEnvelope();
          }
          else {
            //TODO 小于最小像素点 1/4096 ，直接丢弃 ，是否可考虑生成一个最小的像素点 1/4096
            continue;
          }

          GeometryWithTag resultGeom = new GeometryWithTag(geometryWithTag.layer, geometryWithTag.id,
            geometryWithTag.tags, geometryWithTag.group, gridGeometry, geometryWithTag.size, geometryWithTag.area,
            geometryWithTag.hash);
          result.add(resultGeom);
        }
      }
    }
    return result;
  }

  /**
   * 纯纯融合边界，不丢失任何数据
   *
   * @param origin
   * @return
   * @throws GeometryException
   */
  private List<VectorTile.Feature> mergeSameFeatures(List<VectorTile.Feature> origin) throws GeometryException {
    if (origin.isEmpty()) {
      return origin;
    }
    if (origin.getFirst().geometry().geomType() != GeometryType.POLYGON) {
      return origin;
    }
    try {
      return FeatureMerge.mergeOverlappingPolygons(origin, 0);
    } catch (AssertionError e) {
      // TODO: 定位要素解码错误
      LOGGER.error("[processTile] {}", tileCoord, e);
      return Collections.emptyList();
    }
  }

  public record GeometryWrapper(Geometry geometry, double area, VectorTile.Feature feature, int commandLen) {}

  /**
   * 要素融合，会丢失数据
   *
   * @param origin 原始要素列表
   * @param ratio  必需大于等于1.0
   * @return
   */
  private List<VectorTile.Feature> mergeNearbyFeatures(List<VectorTile.Feature> origin, double ratio) {
    if (origin.isEmpty()) {
      return origin;
    }
    if (origin.getFirst().geometry().geomType() != GeometryType.POLYGON) {
      return origin;
    }
    int totalCommand = 0;
    for (VectorTile.Feature feature : origin) {
      totalCommand += feature.geometry().commands().length;
    }
    // TODO: 将这个参数提取到配置中，每一层都有同的参数
    double minDistAndBuffer = 1.0 / 16;
    // TODO: 将这个参数提取到配置中，每一层都有同的参数
    double minArea = 1.0 / 16;
    // 计算需要合并的要素数量
    int shrinkFeatureSize = origin.size() - (int) (origin.size() / ratio);
    int shrinkCommandSize = totalCommand - (int) (totalCommand / ratio);
    if (shrinkFeatureSize == 0) {
      // 不需要删除
      return origin;
    }
    List<GeometryWrapper> allGeometries = new ArrayList<>();
    GeometryFactory factory = new GeometryFactory();
    for (VectorTile.Feature feature : origin) {
      try {
        Geometry decode;
        try {
          decode = feature.geometry().decode();
        } catch (AssertionError e) {
          continue;
        }
        // 根据像素将矢量要素简化
        fastSimplifyFeature(feature, decode, factory, allGeometries);
      } catch (GeometryException e) {
        LOGGER.debug(e.getMessage());
      }
    }
    // TODO: 需要改成全局排序
    allGeometries.sort(Comparator.comparingDouble(GeometryWrapper::area));
    // 1、过滤出面积小于1像素（在4096*4096网格下）
    List<GeometryWrapper> small = new ArrayList<>();
    List<GeometryWrapper> middle = new ArrayList<>();
    List<GeometryWrapper> bigger = new ArrayList<>();
    int commandSize = 0;
    for (GeometryWrapper geometryWrapper : allGeometries) {
      if (commandSize < shrinkCommandSize) {
        if (geometryWrapper.area() < minArea) {
          small.add(geometryWrapper);
        } else {
          middle.add(geometryWrapper);
          commandSize += geometryWrapper.commandLen();
        }
      } else {
        bigger.add(geometryWrapper);
      }
    }
    // TODO: 将中型要素，融合到大型要素上
//    List<GeometryWrapper> merged = featureToGrid(middle);
//    bigger.addAll(merged);
    return bigger.stream().map(w -> w.feature).toList();

//    return bigger.stream()
//      .map(g -> g.feature.copyWithNewGeometry(g.geometry)).toList();
  }

  private static void fastSimplifyFeature(VectorTile.Feature feature, Geometry decode, GeometryFactory factory,
    List<GeometryWrapper> allGeometries) {
    MutableCoordinateSequence sequence = new MutableCoordinateSequence();
    Coordinate[] coordinates = decode.getCoordinates();
    if (coordinates.length == 0) {
      return;
    }
    Coordinate pre = coordinates[0];
    sequence.forceAddPoint(coordinates[0].x, coordinates[0].y);
    for (int i = 1; i < coordinates.length; i++) {
      Coordinate current = coordinates[i];
      if (!(Math.abs(current.x - pre.x) <= 1.0 / 16) || !(Math.abs(current.y - pre.y) <= 1.0 / 16)) {
        sequence.forceAddPoint(current.x, current.y);
      }
      pre = current;
    }
    sequence.forceAddPoint(coordinates[0].x, coordinates[0].y);
    if (sequence.size() < MINIMUM_VALID_SIZE) {
      return;
    }
    // 生成新的面数据
    Polygon polygon = new Polygon(new LinearRing(sequence, factory), new LinearRing[0], factory);
    allGeometries.add(
      new GeometryWrapper(polygon, Double.parseDouble(feature.getTag("Shape_Area").toString()), feature,
        feature.geometry().commands().length));
  }

  private static void mergeMiddleFeatureToBigger(List<GeometryWrapper> bigger, List<GeometryWrapper> middle,
    double minDistAndBuffer) {
    // 构建一个保留要素的索引
    STRtree envelopeIndex = new STRtree();
    for (int i = 0; i < bigger.size(); i++) {
      Geometry geometry = bigger.get(i).geometry();
      Envelope env = geometry.getEnvelopeInternal().copy();
      env.expandBy(0);
      envelopeIndex.insert(env, i);
    }
    // 2、从待合并要素中遍历像素，目标是跟周围要素进行一个融合，不需要保存属性
    for (GeometryWrapper wrapper : middle) {
      Geometry geometry = wrapper.geometry();
      // 存储与待删除要素相交的要素
      Set<Integer> set = new HashSet<>();
      envelopeIndex.query(geometry.getEnvelopeInternal(), object -> {
        if (object instanceof Integer j) {
          Geometry b = bigger.get(j).geometry();
          if (geometry.isWithinDistance(b, minDistAndBuffer)) {
            set.add(j);
          }
        }
      });
      // 将需要删除的要素执行像素化分配给待合并的要素
      List<Integer> list = new ArrayList<>(set);
      list.sort(Comparator.comparingDouble(o -> bigger.get(o).area));
      if (!list.isEmpty()) {
        Integer target = list.getFirst();
        GeometryWrapper targetWrapper = bigger.get(target);
        List<Geometry> mergeList = new ArrayList<>();
        Geometry targetGeometry = targetWrapper.geometry();
        mergeList.add(targetGeometry);
        mergeList.add(geometry.buffer(minDistAndBuffer));
        // TODO: 合并并不一定会成功，需要优化合并算法
        Geometry merged = GeoUtils.createGeometryCollection(mergeList);
        try {
          merged = union(merged);
        } catch (TopologyException e) {
          // 缓冲结果有时无效，这使得 union 抛出异常，因此修复它并重试（见 #700）
          merged = GeometryFixer.fix(merged);
          merged = union(merged);
        }
        // TODO: 对合并完的要素再做一次简化算法
        targetWrapper.feature().copyWithNewGeometry(merged);
      } else {
        // TODO: 执行要素合并
      }
    }
  }
}
