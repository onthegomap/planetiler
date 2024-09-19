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
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.util.AffineTransformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AdvancedLandCoverTiling implements Profile {

  private static final Logger LOGGER = LoggerFactory.getLogger(AdvancedLandCoverTiling.class);
  private static final int MAX_RESOLUTION = 4096;
  /**
   * 默认一个像素占据1x1单元格，pixelSize为2，占据2x2个单元格
   */
  private double pixelSize = 1;
  /**
   * 像素化默认网格数
   */
  private static final int DEFAULT_PIXELATION_GRID_SIZE = 256;
  private int pixelGridSize = 256;
  private static final int DEFAULT_TILE_SIZE = 256;
  private static final double TILE_SCALE = 0.5d;
  private static final int PIXELATION_ZOOM = 12;
  private static final int BATCH_SIZE = 50; // 可以根据实际情况调整
  private static final int BUFFER_SIZE = 4; // 缓冲区大小


  private static final GeometryFactory geometryFactory = new GeometryFactory();
  private PlanetilerConfig config;
  private Mbtiles mbtiles;
  private final ExecutorService executorService = Executors.newFixedThreadPool(
    Runtime.getRuntime().availableProcessors());

  public AdvancedLandCoverTiling(PlanetilerConfig config, Mbtiles mbtiles) {
    this.config = config;
    this.mbtiles = mbtiles;
  }

  public void generateLowerZoomTiles() {
    try (Mbtiles.TileWriter writer = mbtiles.newTileWriter()) {
      for (int zoom = config.maxzoom() - 1; zoom >= config.minzoom(); zoom--) {
        if (zoom + 1 == config.maxzoom()) {
          writer.setTileDataIdCounter(mbtiles.getMaxDataTileId() + 1);
        }

        pixelGridSize = Math.min(getPixelationGridSizeAtZoom(zoom), MAX_RESOLUTION);
        pixelSize = DEFAULT_TILE_SIZE / (double) pixelGridSize;
        LOGGER.debug("zoom:{} pixelGridSize:{} pixelSize:{}", zoom, pixelGridSize, pixelSize);
        processTileBatch(zoom, 0, 0, 0, 0, writer);
//        processZoomLevel(zoom, writer);
        writer.flush();
      }
      updateMetadata();
    } finally {
      executorService.shutdown();
    }
  }

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

    int totalBatches =
      ((maxParentX - minParentX + 1) / BATCH_SIZE + 1) * ((maxParentY - minParentY + 1) / BATCH_SIZE + 1);
    int processedBatches = 0;
    LOGGER.info("处理缩放级别 {} 的父瓦片范围：X({} to {}), Y({} to {}) (TMS)", z, minParentX, maxParentX, minParentY,
      maxParentY);

    for (int parentX = minParentX; parentX <= maxParentX; parentX += BATCH_SIZE) {
      for (int parentY = minParentY; parentY <= maxParentY; parentY += BATCH_SIZE) {
        int endParentX = Math.min(parentX + BATCH_SIZE - 1, maxParentX);
        int endParentY = Math.min(parentY + BATCH_SIZE - 1, maxParentY);

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

        processTileBatch(z, minChildX, minChildY, maxChildX, maxChildY, writer);

        processedBatches++;
        LOGGER.info("缩放级别 {} 处理进度: {}/{} 批次", z, processedBatches, totalBatches);
      }
    }

    long endTime = System.currentTimeMillis();
    LOGGER.info("缩放级别 {} 处理完成，耗时: {} ms", z, (endTime - startTime));
  }

  private void processTileBatch(int z, int minX, int minY, int maxX, int maxY, Mbtiles.TileWriter writer) {
    try (CloseableIterator<Tile> tileIterator = mbtiles.getZoomTiles(z + 1)) {
      List<CompletableFuture<Void>> processTileFutures = new ArrayList<>();
      Map<TileCoord, Map<String, ConcurrentLinkedQueue<VectorTile.Feature>>> currentZoomTiles = new ConcurrentHashMap<>();

      // 将所有要素进行仿真变换
      while (tileIterator.hasNext()) {
        Tile next = tileIterator.next();
        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> processTile(next, currentZoomTiles),
          executorService);
        processTileFutures.add(future);
      }
      CompletableFuture.allOf(processTileFutures.toArray(new CompletableFuture[0])).join();

      // 要素栅格化
      List<CompletableFuture<TileEncodingResult>> encodingFutures = new ArrayList<>();
      for (Map.Entry<TileCoord, Map<String, ConcurrentLinkedQueue<VectorTile.Feature>>> entry : currentZoomTiles.entrySet()) {
        encodingFutures.add(
          CompletableFuture.supplyAsync(() -> encodeTile(entry.getKey(), entry.getValue(), z), executorService));
      }

      CompletableFuture.allOf(encodingFutures.toArray(new CompletableFuture[0])).join();
      encodingFutures.stream()
        .map(CompletableFuture::join)
        .forEach(writer::write);
    } catch (Exception e) {
      LOGGER.error("处理瓦片批次 ({},{}) 到 ({},{}) 在缩放级别 {} 时发生错误",
        minX, minY, maxX, maxY, z, e);
    }
  }

  /**
   * 处理瓦片进行仿真变换
   *
   * @param tile
   * @param currentZoomTiles
   */
  private void processTile(Tile tile,
    Map<TileCoord, Map<String, ConcurrentLinkedQueue<VectorTile.Feature>>> currentZoomTiles) {
    try {
      TileCoord parentCoord = tile.coord().parent();
      List<VectorTile.Feature> features = VectorTile.decode(Gzip.gunzip(tile.bytes()));

      for (VectorTile.Feature feature : features) {
        Object name = feature.getTag("name");
        Geometry processedGeometry = simulationTransformation(feature.geometry().decode(), tile.coord());

        if (!processedGeometry.isEmpty()) {
          VectorTile.VectorGeometry vectorGeometry = VectorTile.encodeGeometry(processedGeometry, 5);
          if (vectorGeometry.isEmpty()) {
            LOGGER.warn("处理瓦片 {} 时，无法处理要素 {}，其几何图形为空", tile.coord(), name);
            continue;
          }

          currentZoomTiles.computeIfAbsent(parentCoord, k -> new ConcurrentHashMap<>())
            .computeIfAbsent(feature.layer(), k -> new ConcurrentLinkedQueue<>())
            .add(feature.copyWithNewGeometry(vectorGeometry));
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

    Geometry geom = transform.scale(TILE_SCALE, TILE_SCALE)
      .translate(translateX, translateY)
      .transform(geometry);
    if (geom.isEmpty()) {
      LOGGER.error("仿真变换后要素为空！");
    }

    if (!geom.isValid()) {
      geom = GeoUtils.fixPolygon(geom);
    }
    return geom;
  }

  private TileEncodingResult encodeTile(TileCoord coord, Map<String, ConcurrentLinkedQueue<VectorTile.Feature>> layers,
    int z) {
    try {
      VectorTile vectorTile = new VectorTile();
      for (Map.Entry<String, ConcurrentLinkedQueue<VectorTile.Feature>> layer : layers.entrySet()) {
        List<VectorTile.Feature> vectorGeometryList = rasterizeFeatures(layer.getValue(), z).stream()
          .map(feature -> feature.copyWithNewGeometry(feature.geometry().unscale()))
          .toList();

        vectorTile.addLayerFeatures(layer.getKey(), vectorGeometryList);
      }
      return new TileEncodingResult(coord, Gzip.gzip(vectorTile.encode()), OptionalLong.empty());
    } catch (Exception e) {
      LOGGER.error("编码瓦片 {} 时发生错误", coord, e);
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
  private List<VectorTile.Feature> rasterizeFeatures(ConcurrentLinkedQueue<VectorTile.Feature> features, int zoom)
    throws GeometryException {
    if (zoom > -1) {
      return FeatureMerge.mergeOverlappingPolygons(List.copyOf(features), 0);
    }

    // 构建索引
    PolygonIndex<VectorTile.Feature> featureIndex = PolygonIndex.create();
    for (VectorTile.Feature feature : features) {
      featureIndex.put(feature.geometry().decode(), feature);
    }

    ConcurrentHashMap<String, FeatureInfo> pixelFeatures = new ConcurrentHashMap<>();
    pixelationGeometry(pixelFeatures, featureIndex);

    List<VectorTile.Feature> list = pixelFeatures.values().stream()
      .map(info -> info.feature.copyWithNewGeometry(info.feature.geometry())).toList();
    return FeatureMerge.mergeOverlappingPolygons(list, 0);
  }


  private void pixelationGeometry(ConcurrentHashMap<String, FeatureInfo> pixelFeatures,
    PolygonIndex<VectorTile.Feature> featureIndex)
    throws GeometryException {

    // 简单扩充边界避免瓦片出现网格线的问题
    for (int x = -BUFFER_SIZE; x < pixelGridSize + BUFFER_SIZE; x++) {
      for (int y = -BUFFER_SIZE; y < pixelGridSize + BUFFER_SIZE; y++) {
        Geometry pixelPolygon = createPixelGeometry(x, y);
        List<VectorTile.Feature> intersectingFeatures = featureIndex.getIntersecting(pixelPolygon);

        if (!intersectingFeatures.isEmpty()) {
          VectorTile.Feature bestFeature = null;
          double maxIntersectionArea = 0;

          for (VectorTile.Feature feature : intersectingFeatures) {
            Geometry decode = feature.geometry().decode();
            // TODO 可能是解码和编码精度问题导致此处需要再次修复无效几何体
            if (!decode.isValid()) {
              decode = GeoUtils.fixPolygon(decode);
              LOGGER.error("再次修复, 原因未知！！！");
            }
            Geometry intersection = decode.intersection(pixelPolygon);
            double intersectionArea = intersection.getArea();

            if (intersectionArea > maxIntersectionArea) {
              maxIntersectionArea = intersectionArea;
              bestFeature = feature;
            }
          }

          if (bestFeature != null) {
            String pixelKey = x + ":" + y;
            pixelFeatures.put(pixelKey, new FeatureInfo(
              bestFeature.copyWithNewGeometry(VectorTile.encodeGeometry(pixelPolygon)),
              pixelPolygon.getArea(),
              maxIntersectionArea,
              pixelKey
            ));
          }
        }
      }
    }
  }

  private Geometry createPixelGeometry(double x, double y) {
    double minX = x * pixelSize;
    double minY = y * pixelSize;
    double maxX = (x + 1) * pixelSize;
    double maxY = (y + 1) * pixelSize;

    return geometryFactory.createPolygon(new Coordinate[]{
      new Coordinate(minX, minY),
      new Coordinate(maxX, minY),
      new Coordinate(maxX, maxY),
      new Coordinate(minX, maxY),
      new Coordinate(minX, minY)
    });
  }

  private void updateMetadata() {
    Mbtiles.Metadata metadata = mbtiles.metadataTable();
    TileArchiveMetadata archiveMetadata = metadata.get();
    TileArchiveMetadata.TileArchiveMetadataJson metadataJson = TileArchiveMetadata.TileArchiveMetadataJson.create(
      archiveMetadata.json().vectorLayers().stream()
        .map(vectorLayer -> vectorLayer.withMinzoom(config.minzoom()))
        .toList()
    );
    metadata.updateMetadata(
      Map.of(
        "minzoom", String.valueOf(config.minzoom()),
        "json", JsonUitls.toJsonString(metadataJson)
      )
    );
  }

  public int getPixelationGridSizeAtZoom(int zoom) {
    return ZoomFunction.applyAsIntOrElse(config.pixelationGridSizeOverrides(), zoom, DEFAULT_PIXELATION_GRID_SIZE);
  }

  @Override
  public void processFeature(SourceFeature sourceFeature, FeatureCollector features) {
  }

  @Override
  public Map<String, List<VectorTile.Feature>> postProcessTileFeatures(TileCoord tileCoord,
    Map<String, List<VectorTile.Feature>> layers) throws GeometryException {
    for (Map.Entry<String, List<VectorTile.Feature>> next : layers.entrySet()) {
      List<VectorTile.Feature> features = rasterizeFeatures(new ConcurrentLinkedQueue<>(next.getValue()), 1);
      next.setValue(features);
    }

    return layers;
  }

  private record FeatureInfo(
    VectorTile.Feature feature,
    double area,
    double intersectionArea,
    String pixel
  ) {}

  /**
   * 主方法，用于测试和运行切片生成过程
   */
  public static void main(String[] args) {
    String mbtilesPath = "E:\\Linespace\\SceneMapServer\\Data\\parquet\\shanghai\\default-14\\default-14 - 副本.mbtiles";
    PlanetilerConfig planetilerConfig = PlanetilerConfig.from(
      Arguments.of(
        "minzoom", 0,
        "maxzoom", 14,
        "pixelation_grid_size_overrides", "12=256",
        "bounds", "120.65834964097692, 30.358135461680284,122.98862516825757,32.026462694269135"));
    try (WriteableTileArchive archive = TileArchives.newWriter(Paths.get(mbtilesPath), planetilerConfig)) {
      AdvancedLandCoverTiling tiling = new AdvancedLandCoverTiling(planetilerConfig, (Mbtiles) archive);

      tiling.generateLowerZoomTiles();
      LOGGER.info("所有缩放级别处理完成");
    } catch (Exception e) {
      LOGGER.error("处理过程中发生错误", e);
    }
  }

}
