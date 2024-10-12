package com.onthegomap.planetiler.render;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.FeatureMerge;
import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.VectorTile;
import com.onthegomap.planetiler.archive.Tile;
import com.onthegomap.planetiler.archive.TileArchiveMetadata;
import com.onthegomap.planetiler.archive.TileArchives;
import com.onthegomap.planetiler.archive.TileEncodingResult;
import com.onthegomap.planetiler.archive.WriteableTileArchive;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.config.Bounds;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.DouglasPeuckerSimplifier;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.geo.PolygonIndex;
import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.geo.TileExtents;
import com.onthegomap.planetiler.mbtiles.Mbtiles;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.util.CloseableIterator;
import com.onthegomap.planetiler.util.Gzip;
import com.onthegomap.planetiler.util.JsonUitls;
import com.onthegomap.planetiler.util.ZoomFunction;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import org.apache.commons.lang3.time.StopWatch;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.util.AffineTransformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AdvancedLandCoverTile implements Profile {

  private static final Logger LOGGER = LoggerFactory.getLogger(AdvancedLandCoverTile.class);

  private static final int SCALE_FACTOR = 100000; // 4位小数精度
  private static final int MAX_RESOLUTION = 4096;
  private static final int DEFAULT_TILE_SIZE = 256;
  private static final double TILE_SCALE = 0.5d;
  private static final int BUFFER_SIZE = 1;
  private static final GeometryFactory geometryFactory = new GeometryFactory();
  private final PlanetilerConfig config;

  private final int THREAD_NUM = Runtime.getRuntime().availableProcessors();

  private final ExecutorService executorService = new ThreadPoolExecutor(THREAD_NUM, THREAD_NUM, 10, TimeUnit.SECONDS,
    new LinkedBlockingQueue<>(), new ThreadPoolExecutor.CallerRunsPolicy());

  private final AtomicInteger count = new AtomicInteger(0);

  /**
   * 默认一个像素占据1x1单元格，pixelSize为2，占据2x2个单元格
   */
  private double pixelSize = 1;
  /**
   * 像素化默认网格数
   */
  private int pixelGridSize = 256;
  /**
   * 该缓存消耗内存较大，合理设置网格集大小。每个 Object 对象大约需要 300 字节
   * <p>
   * 预估各网格集缓存大小如下： - 256 * 256 ：20M - 512 * 512 ：70M - 1024 * 1024：310M - 2048 * 2048 : 1.2G
   */
  private final ConcurrentHashMap<String, Object[]> cachePixelGeom = new ConcurrentHashMap<>();
  private final Map<TileCoord, Map<String, ConcurrentLinkedQueue<FeatureInfo>>> currentZoomTiles = new ConcurrentHashMap<>();
  private Mbtiles mbtiles;

  public AdvancedLandCoverTile(PlanetilerConfig config, Mbtiles mbtiles) {
    this.config = config;
    this.mbtiles = mbtiles;
  }

  public AdvancedLandCoverTile(PlanetilerConfig config) {
    this.config = config;
  }


  public void run() throws IOException {
    LOGGER.info("开始进行像素化操作！");
    StopWatch stopWatch = new StopWatch();
    stopWatch.start();
    try (Mbtiles.TileWriter writer = mbtiles.newTileWriter()) {
      // 记录已经缓存过得网格集
      List<Integer> pixelGridList = new ArrayList<>();
      for (int zoom = config.rasterizeMaxZoom() - 1; zoom >= config.rasterizeMinZoom(); zoom--) {
        if (zoom + 1 == config.rasterizeMaxZoom()) {
          writer.setTileDataIdCounter(mbtiles.getMaxDataTileId() + 1);
        }
        initPixelSize(zoom);

        if (zoom <= config.pixelationZoom() && !pixelGridList.contains(pixelGridSize)) {
          initCachePixelGeom();
          pixelGridList.add(pixelGridSize);
        }

//        processTileBatch(zoom, 0, 0, 0, 0, writer);
        processZoomLevel(zoom, writer);
        writer.flush();
      }
      updateMetadata();
    } finally {
      executorService.shutdown();
      mbtiles.close();
    }

    stopWatch.stop();
    LOGGER.info("所有缩放级别处理完成, 总消耗：{}", stopWatch.getTime(TimeUnit.SECONDS));

  }

  /**
   * 初始化网格集，提高效率
   */
  private void initCachePixelGeom() {
    int maxCoord = DEFAULT_TILE_SIZE + BUFFER_SIZE;

    DoubleStream.iterate(-BUFFER_SIZE, x -> x + pixelSize).limit((long) Math.ceil((maxCoord + BUFFER_SIZE) / pixelSize))
      .parallel().forEach(x -> {
        for (double y = -BUFFER_SIZE; y < maxCoord; y += pixelSize) {
          Geometry pixelGeometry = createPixelGeometry(x, y);
          cachePixelGeom.put(generateKey(x, y), new Object[]{pixelGeometry, VectorTile.encodeGeometry(pixelGeometry)});
        }
      });
  }


  public AdvancedLandCoverTile initPixelSize(int zoom) {
    pixelGridSize = Math.min(getPixelationGridSizeAtZoom(zoom), MAX_RESOLUTION);
    pixelSize = DEFAULT_TILE_SIZE / (double) pixelGridSize;
    LOGGER.info("zoom:{} pixelGridSize:{} pixelSize:{}", zoom, pixelGridSize, pixelSize);
    return this;
  }


  /**
   * 分批处理高层级要素，避免一次读取导致内存溢出
   *
   * @param z
   * @param writer
   */
  public void processZoomLevel(int z, Mbtiles.TileWriter writer) {
    LOGGER.info("开始处理缩放级别 {}", z);
    long startTime = System.currentTimeMillis();
    Bounds bounds = config.bounds();
    TileExtents.ForZoom currentZoom = bounds.tileExtents().getForZoom(z);
    TileExtents.ForZoom highZoom = bounds.tileExtents().getForZoom(z + 1);

    // 计算父瓦片的范围 (TMS坐标)
    int minParentX = currentZoom.minX();
    int maxParentX = currentZoom.maxX();
    int minParentY = currentZoom.minY();
    int maxParentY = currentZoom.maxY();

    // 计算层级下最大瓦片数
    int currentMaxY = (1 << z) - 1;
    int highMaxY = (1 << (z + 1)) - 1;

    int tileBatchSize = config.tileBatchSize();
    int totalBatches =
      ((maxParentX - minParentX + 1) / tileBatchSize + 1) * ((maxParentY - minParentY + 1) / tileBatchSize + 1);
    int processedBatches = 0;
    LOGGER.info("处理缩放级别 {} 的父瓦片范围：X({} to {}), Y({} to {}) (TMS)", z, minParentX, maxParentX, minParentY,
      maxParentY);
    // 控制并发的版本
    processByParentTiles(z, writer);

    // 第一个版本
//    beforeParentTiles(z, writer, minParentX, maxParentX, tileBatchSize, minParentY, maxParentY, currentMaxY, highZoom, highMaxY,
//      processedBatches, totalBatches);

    count.set(0);
    long endTime = System.currentTimeMillis();
    LOGGER.info("缩放级别 {} 处理完成，耗时: {} ms", z, (endTime - startTime));
  }

  private void beforeParentTiles(int z, Mbtiles.TileWriter writer, int minParentX, int maxParentX, int tileBatchSize,
    int minParentY, int maxParentY, int currentMaxY, TileExtents.ForZoom highZoom, int highMaxY, int processedBatches,
    int totalBatches) {
    for (int parentX = minParentX; parentX <= maxParentX; parentX += tileBatchSize) {
      for (int parentY = minParentY; parentY <= maxParentY; parentY += tileBatchSize) {
        int endParentX = Math.min(parentX + tileBatchSize - 1, maxParentX);
        int endParentY = Math.min(parentY + tileBatchSize - 1, maxParentY);

        // 计算子瓦片的范围 (XYZ坐标)
        int minChildX = parentX * 2;
        // + 1 的作用是确保计算出的范围包括了父级瓦片对应的所有子级瓦片，覆盖了父级瓦片的整个高度。 -1因为索引从零开始
        int maxChildX = (endParentX + 1) * 2 - 1;
        int minChildY = (currentMaxY - endParentY) * 2;  // 翻转Y坐标并计算最小Y
        int maxChildY = (currentMaxY - parentY + 1) * 2 - 1;  // 翻转Y坐标并计算最大Y

        // 确保子瓦片范围不超出高层级的实际范围
        minChildX = Math.max(minChildX, highZoom.minX());
        maxChildX = Math.min(maxChildX, highZoom.maxX());
        maxChildY = Math.min(maxChildY, highMaxY);

        LOGGER.info("处理缩放级别 {} 的子瓦片范围：X({} to {}), Y({} to {}) (TMS)",
          z, minChildX, maxChildX, minChildY, maxChildY);
        processTileBatch(z, minChildX, minChildY, maxChildX, maxChildY, writer);
        LOGGER.info("缩放级别 {} 处理进度: {}/{} 批次", z, processedBatches++, totalBatches);
      }
    }
  }

  private void processByParentTiles(int z, Mbtiles.TileWriter writer) {
    CloseableIterator<TileCoord> allTileCoords = mbtiles.getAllTileCoords();
    Set<TileCoord> tileCoords = allTileCoords.stream().filter(tileCoord -> tileCoord.z() == z + 1)
      .collect(Collectors.toSet());
    Set<TileCoord> parents = new HashSet<>();
    for (TileCoord tileCoord : tileCoords) {
      parents.add(tileCoord.parent());
    }
    LinkedBlockingQueue<TileMergeRunnable> queue = new LinkedBlockingQueue<>(THREAD_NUM);
    // 总体任务数量
    AtomicInteger atomicInteger = new AtomicInteger(parents.size());
    // 异步队列
    Runnable runnable = () -> {
      for (TileCoord parent : parents) {
        try {
          TileMergeRunnable tileMergeRunnable = new TileMergeRunnable(parent, mbtiles, writer, config);
          // 阻塞队列，保证并发数量，内存不会暴增
          queue.put(tileMergeRunnable);
        } catch (Exception e) {
          throw new RuntimeException(e);
        } finally {
          atomicInteger.decrementAndGet();
        }
      }
    };
    new Thread(runnable, "TileMergeRunnableProducer").start();
    List<CompletableFuture<Void>> processTileFutures = new ArrayList<>();
    for (int i = 0; i < THREAD_NUM; i++) {
      CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
        while (atomicInteger.get() != 0) {
          TileMergeRunnable tileMergeRunnable = null;
          try {
            tileMergeRunnable = queue.poll(10, TimeUnit.MILLISECONDS);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
          if (tileMergeRunnable != null) {
            tileMergeRunnable.run();
          }
        }
      }, executorService);
      processTileFutures.add(future);
    }
    CompletableFuture.allOf(processTileFutures.toArray(new CompletableFuture[0])).join();
  }

  private void processTileBatch(int z, int minX, int minY, int maxX, int maxY, Mbtiles.TileWriter writer) {
    try (CloseableIterator<Tile> tileIterator = mbtiles.getZoomTiles(z + 1, minX, minY, maxX, maxY)) {
      List<CompletableFuture<Void>> processTileFutures = new ArrayList<>();

      // 将所有要素进行仿真变换
      while (tileIterator.hasNext()) {
        Tile next = tileIterator.next();
        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> processTile(next), executorService);
        processTileFutures.add(future);
      }
      CompletableFuture.allOf(processTileFutures.toArray(new CompletableFuture[0])).join();

      // 要素栅格化
      List<CompletableFuture<TileEncodingResult>> encodingFutures = new ArrayList<>();
      for (Map.Entry<TileCoord, Map<String, ConcurrentLinkedQueue<FeatureInfo>>> entry : currentZoomTiles.entrySet()) {
        encodingFutures.add(
          CompletableFuture.supplyAsync(() -> encodeTile(entry.getKey(), entry.getValue(), z), executorService));
      }

      CompletableFuture.allOf(encodingFutures.toArray(new CompletableFuture[0])).join();
      encodingFutures.stream().map(CompletableFuture::join).forEach(writer::write);
    } catch (Exception e) {
      LOGGER.error("处理瓦片批次 ({},{}) 到 ({},{}) 在缩放级别 {} 时发生错误", minX, minY, maxX, maxY, z, e);
    } finally {
      currentZoomTiles.clear();
    }
  }

  /**
   * 处理瓦片进行仿真变换
   *
   * @param tile
   */
  private void processTile(Tile tile) {
    try {
      TileCoord parentCoord = tile.coord().parent();
      List<VectorTile.Feature> features = VectorTile.decode(Gzip.gunzip(tile.bytes()));

      for (VectorTile.Feature feature : features) {
        Geometry transformationGeom = simulationTransformation(feature.geometry().decode(), tile.coord());

        if (!transformationGeom.isEmpty()) {
          VectorTile.VectorGeometry vectorGeometry = VectorTile.encodeGeometry(transformationGeom, 5);
          if (vectorGeometry.isEmpty()) {
            LOGGER.warn("处理瓦片 {} 时，无法处理要素，其几何图形为空", tile.coord());
            continue;
          }

          currentZoomTiles.computeIfAbsent(parentCoord, k -> new ConcurrentHashMap<>())
            .computeIfAbsent(feature.layer(), k -> new ConcurrentLinkedQueue<>()).add(
              new FeatureInfo(feature.copyWithNewGeometry(vectorGeometry), transformationGeom,
                transformationGeom.getArea()));
          LOGGER.debug("tile:{},currentZoomTiles {}", tile, currentZoomTiles.size());
        }
      }
    } catch (Exception e) {
      LOGGER.error(String.format("处理瓦片 %s 时发生错误", tile.coord()), e);
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

    return DouglasPeuckerSimplifier.simplify(geom, 0.0625);
  }


  private TileEncodingResult encodeTile(TileCoord coord, Map<String, ConcurrentLinkedQueue<FeatureInfo>> layers,
    int z) {
    try {
      VectorTile vectorTile = new VectorTile();
      for (Map.Entry<String, ConcurrentLinkedQueue<FeatureInfo>> layer : layers.entrySet()) {
        LOGGER.debug("开始处理瓦片：{}，当前层级瓦片已处理：{} 个", coord, count.addAndGet(1));
        List<VectorTile.Feature> vectorGeometryList = rasterizeFeatures(new ArrayList<>(layer.getValue()), z).stream()
          .map(feature -> feature.copyWithNewGeometry(feature.geometry().unscale())).toList();

        vectorTile.addLayerFeatures(layer.getKey(), vectorGeometryList);
      }
      return new TileEncodingResult(coord, Gzip.gzip(vectorTile.encode()), OptionalLong.empty());
    } catch (Exception e) {
      LOGGER.error(String.format("编码瓦片 %s 时发生错误", coord), e);
      return null;
    }
  }

  /**
   * 要素栅格化
   *
   * @param features
   * @param zoom
   * @return
   * @throws GeometryException
   */
  public List<VectorTile.Feature> rasterizeFeatures(List<FeatureInfo> features, int zoom) throws GeometryException {
    if (zoom > config.pixelationZoom()) {
      List<VectorTile.Feature> list = features.stream().map(FeatureInfo::feature).toList();
      return FeatureMerge.mergeOverlappingPolygons(list, 0);
    }

    // 构建索引
    PolygonIndex<Integer> featureIndex = PolygonIndex.create();
    for (int i = 0; i < features.size(); i++) {
      FeatureInfo featureInfo = features.get(i);
      featureIndex.put(featureInfo.geometry, i);
    }

    List<VectorTile.Feature> pixelFeatures = pixelationGeometry(featureIndex, features);
    return FeatureMerge.mergeOverlappingPolygons(pixelFeatures, 0);
  }

  /**
   * 网格相交计算：非常耗时
   *
   * @param featureIndex
   * @return
   * @throws GeometryException
   */
  public List<VectorTile.Feature> pixelationGeometry(PolygonIndex<Integer> featureIndex, List<FeatureInfo> featureInfos)
    throws GeometryException {
    List<VectorTile.Feature> pixelFeatures = new ArrayList<>();

    // 计算每个网格平均保存的要素数量
    int pixelMaxFeatures = Math.max(1, config.maxFeatures() / (pixelGridSize * pixelGridSize));
    // 计算剩余还可以保存的数量
    int remain = config.maxFeatures() - pixelMaxFeatures * pixelGridSize * pixelGridSize;
    int batchSize = 2000;

    // 计算实际的网格数量
    for (double x = -BUFFER_SIZE; x < DEFAULT_TILE_SIZE + BUFFER_SIZE; x += pixelSize) {
      for (double y = -BUFFER_SIZE; y < DEFAULT_TILE_SIZE + BUFFER_SIZE; y += pixelSize) {
        Object[] objects = cachePixelGeom.get(generateKey(x, y));
        Geometry pixelPolygon = (Geometry) objects[0];
        // TODO: 计算过慢
        List<Integer> intersecting = featureIndex.getIntersecting(pixelPolygon);
        if (intersecting.isEmpty()) {
          continue;
        }

        List<FeatureInfo> intersectionFeatures = intersecting.stream()
          .sorted(Comparator.comparingDouble(f -> -featureInfos.get(f).area))
          .map(featureInfos::get)
          .limit(pixelMaxFeatures)
          .toList();
        // 选择此像素的特征
//        List<FeatureInfo> intersectionFeatures = intersecting.stream()
//          .sorted(Comparator.comparingDouble(f -> -f.area))
//          .limit(pixelMaxFeatures)
//          .toList();
//
//        // TODO 后续提高精度此处换为相交计算
        intersectionFeatures.forEach(info ->
          pixelFeatures.add(info.feature.copyWithNewGeometry((VectorTile.VectorGeometry) objects[1]))
        );

//        if (count++ / batchSize  == 0) {
//          FeatureMerge.mergeOverlappingPolygons(pixelFeatures, 0);
//          pixelFeatures.clear();
//        }
      }
    }

    return pixelFeatures;
  }


  /**
   * 防止double类型转换精度丢失
   *
   * @param x
   * @param y
   * @return
   */
  public String generateKey(double x, double y) {
    int scaledX = (int) Math.round(x * SCALE_FACTOR);
    int scaledY = (int) Math.round(y * SCALE_FACTOR);
    int scaledSize = (int) Math.round(pixelSize * SCALE_FACTOR);

    return scaledX + ":" + scaledY + ":" + scaledSize;
  }


  private Geometry createPixelGeometry(double x, double y) {
    double minX = x * pixelSize;
    double minY = y * pixelSize;
    double maxX = (x + 1) * pixelSize;
    double maxY = (y + 1) * pixelSize;

    return geometryFactory.createPolygon(
      new Coordinate[]{new Coordinate(minX, minY), new Coordinate(maxX, minY), new Coordinate(maxX, maxY),
        new Coordinate(minX, maxY), new Coordinate(minX, minY)}).reverse();
  }

  private void updateMetadata() {
    Mbtiles.Metadata metadata = mbtiles.metadataTable();
    TileArchiveMetadata archiveMetadata = metadata.get();
    TileArchiveMetadata.TileArchiveMetadataJson metadataJson = TileArchiveMetadata.TileArchiveMetadataJson.create(
      archiveMetadata.json().vectorLayers().stream()
        .map(vectorLayer -> vectorLayer.withMinzoom(config.rasterizeMinZoom())).toList());
    metadata.updateMetadata(
      Map.of("minzoom", String.valueOf(config.rasterizeMinZoom()), "json", JsonUitls.toJsonString(metadataJson)));
  }

  public int getPixelationGridSizeAtZoom(int zoom) {
    return ZoomFunction.applyAsIntOrElse(config.pixelationGridSizeOverrides(), zoom, DEFAULT_TILE_SIZE);
  }

  @Override
  public void processFeature(SourceFeature sourceFeature, FeatureCollector features) {
  }

  @Override
  public Map<String, List<VectorTile.Feature>> postProcessTileFeatures(TileCoord tileCoord,
    Map<String, List<VectorTile.Feature>> layers) throws GeometryException {
    for (Map.Entry<String, List<VectorTile.Feature>> next : layers.entrySet()) {
//      List<VectorTile.Feature> features = rasterizeFeatures(new ConcurrentLinkedQueue<>(next.getValue()), 1);
//      next.setValue(features);
    }

    return layers;
  }

  public record FeatureInfo(
    VectorTile.Feature feature, Geometry geometry, double area
  ) {}

  public static AdvancedLandCoverTile create(PlanetilerConfig config, String mbtilesPath) {
    LOGGER.info("config:{}, \n  mbtilesPath:{}", config, mbtilesPath);
    try {
      return new AdvancedLandCoverTile(config, (Mbtiles) TileArchives.newWriter(Paths.get(mbtilesPath), config));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }


  /**
   * 主方法，用于测试和运行切片生成过程
   */
  public static void main(String[] args) {
    String mbtilesPath = "E:\\Linespace\\SceneMapServer\\Data\\parquet\\shanghai\\default-14\\default-14 - 副本.mbtiles";
    PlanetilerConfig planetilerConfig = PlanetilerConfig.from(
      Arguments.of(
        "rasterize_min_zoom", 0
        , "rasterize_max_zoom", 14
        , "bounds", "120.65834964097692, 30.358135461680284,122.98862516825757,32.026462694269135"
        , "pixelation_zoom", 12
//        , "pixelation_grid_size_overrides", "12=512"
      )
    );
    try (WriteableTileArchive archive = TileArchives.newWriter(Paths.get(mbtilesPath), planetilerConfig)) {
      AdvancedLandCoverTile tiling = new AdvancedLandCoverTile(planetilerConfig, (Mbtiles) archive);

      tiling.run();
      LOGGER.info("所有缩放级别处理完成");
    } catch (Exception e) {
      LOGGER.error("处理过程中发生错误", e);
    }
  }

}
