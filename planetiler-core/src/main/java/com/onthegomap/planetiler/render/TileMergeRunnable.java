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
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
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

  private static final int EXTENT = 4096;

  private static final int EXTENT_HALF = 2048;

  private final TileCoord tileCoord;

  private final Mbtiles mbtiles;

  private final Mbtiles.TileWriter writer;

  private final PlanetilerConfig config;

  Map<String, List<VectorTile.Feature>> originFeatureInfos = new ConcurrentHashMap<>();

  public TileMergeRunnable(TileCoord tileCoord, Mbtiles mbtiles, Mbtiles.TileWriter writer, PlanetilerConfig config) {
    this.tileCoord = tileCoord;
    this.mbtiles = mbtiles;
    this.writer = writer;
    this.config = config;
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
      int totalSize = 0;
      while (tileIterator.hasNext()) {
        Tile next = tileIterator.next();
        byte[] bytes = next.bytes();
        totalSize += bytes.length;
        processTile(next, z);
      }
      VectorTile mergedTile = new VectorTile();
      VectorTile reduceTile = new VectorTile();
      int maxSize = config.maxFeatures();
      double ratio = Math.max((double) totalSize / (double) maxSize, 1.0); // 表示需要压缩多少倍数据
      int from = 0;
      int to = 0;
      for (Map.Entry<String, List<VectorTile.Feature>> entry : originFeatureInfos.entrySet()) {
        // 2. 相同属性要素合并，不会产生空洞，不会产生数据丢失
        // 纯纯融合边界，不丢失任何数据
        List<VectorTile.Feature> merged = mergeSameFeatures(entry.getValue());
        mergedTile.addLayerFeatures(entry.getKey(), merged);
        from += merged.size();
        // 3. 要素重新排序、合并，目标是减少瓦片大小，根据瓦片大小进行，设置=2MB
        List<VectorTile.Feature> reduced = mergeNearbyFeatures(merged, ratio);
        reduceTile.addLayerFeatures(entry.getKey(), reduced);
        to += reduced.size();
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
        Geometry decode = feature.geometry().decode();
        Geometry transformationGeom = simulationTransformation(decode, tile.coord());
        if (!transformationGeom.isEmpty()) {
          VectorTile.VectorGeometry vectorGeometry = VectorTile.encodeGeometry(transformationGeom, 5);
          originFeatureInfos.computeIfAbsent(feature.layer(), k -> new ArrayList<>())
            .add(feature.copyWithNewGeometry(vectorGeometry));
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
    double translateX = relativeX * DEFAULT_TILE_SIZE * TILE_SCALE;
    double translateY = relativeY * DEFAULT_TILE_SIZE * TILE_SCALE;

    Geometry geom = transform.scale(TILE_SCALE, TILE_SCALE).translate(translateX, translateY).transform(geometry);
    if (geom.isEmpty()) {
      LOGGER.error("仿真变换后要素为空！");
    }

    if (!geom.isValid()) {
      geom = GeoUtils.fixPolygon(geom);
    }

    return geom;
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

  public record GeometryWrapper(Geometry geometry, double area, VectorTile.Feature feature) {}

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
    // TODO: 将这个参数提取到配置中，每一层都有同的参数
    double minDistAndBuffer = 1.0 / 16;
    // TODO: 将这个参数提取到配置中，每一层都有同的参数
    double minArea = 1.0;
    // 计算需要合并的要素数量
    int shrinkFeatureSize = origin.size() - (int) (origin.size() / ratio);
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
        allGeometries.add(new GeometryWrapper(decode, decode.getArea(), feature));
        // 根据像素将矢量要素简化
        fastSimplifyFeature(feature, decode, factory, allGeometries);
      } catch (GeometryException e) {
        LOGGER.debug(e.getMessage());
      }
    }
    allGeometries.sort(Comparator.comparingDouble(GeometryWrapper::area));
    // 1、过滤出面积小于1像素（在4096*4096网格下）
    List<GeometryWrapper> small = new ArrayList<>();
    List<GeometryWrapper> middle = new ArrayList<>();
    List<GeometryWrapper> bigger = new ArrayList<>();
    int count = 0;
    for (GeometryWrapper geometryWrapper : allGeometries) {
      if (count < shrinkFeatureSize) {
        if (geometryWrapper.area() < minArea) {
          small.add(geometryWrapper);
        } else {
          middle.add(geometryWrapper);
          count++;
        }
      } else {
        bigger.add(geometryWrapper);
      }
    }
    // 将中型要素，融合到大型要素上
    mergeMiddleFeatureToBigger(bigger, middle, minDistAndBuffer);
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
    allGeometries.add(new GeometryWrapper(polygon, polygon.getArea(), feature));
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
