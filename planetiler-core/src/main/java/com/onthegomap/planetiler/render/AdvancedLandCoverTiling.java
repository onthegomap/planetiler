package com.onthegomap.planetiler.render;

import com.onthegomap.planetiler.FeatureMerge;
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
import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.geo.TileExtents;
import com.onthegomap.planetiler.mbtiles.Mbtiles;
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
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.util.AffineTransformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AdvancedLandCoverTiling {

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
  private int pixelGridSize;
  private static final int DEFAULT_TILE_SIZE = 256;
  private static final double TILE_SCALE = 0.5d;
  private static final int PIXELATION_ZOOM = 12;
  private static final int BATCH_SIZE = 50; // 可以根据实际情况调整


  private static final GeometryFactory geometryFactory = new GeometryFactory();
  private final PlanetilerConfig config;
  private final Mbtiles mbtiles;
  private final ExecutorService executorService;
  private Geometry[][] pixelGridArray;


  public AdvancedLandCoverTiling(PlanetilerConfig config, Mbtiles mbtiles) {
    this.config = config;
    this.mbtiles = mbtiles;
    this.executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
  }

  public void generateLowerZoomTiles() {
    try (Mbtiles.TileWriter writer = mbtiles.newTileWriter()) {
      for (int zoom = config.maxzoom() - 1; zoom >= config.minzoom(); zoom--) {
        if (zoom + 1 == config.maxzoom()) {
          writer.setTileDataIdCounter(mbtiles.getMaxDataTileId() + 1);
        }

        initPixelSize(zoom);
        processZoomLevel(zoom, writer);
        writer.flush();
      }
      updateMetadata();
    } finally {
      executorService.shutdown();
    }
  }

  private void initPixelSize(int zoom) {
    pixelGridSize = Math.min(getPixelationGridSizeAtZoom(zoom), MAX_RESOLUTION);
    pixelSize = DEFAULT_TILE_SIZE / (double) pixelGridSize;
    pixelGridArray = new Geometry[pixelGridSize][pixelGridSize];

    LOGGER.debug("zoom:{} pixelGridSize:{} pixelSize:{}", zoom, pixelGridSize, pixelSize);
    for (int x = 0; x < pixelGridSize; x++) {
      for (int y = 0; y < pixelGridSize; y++) {
        pixelGridArray[x][y] = createPixelGeometry(x, y);
      }
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
        int maxChildX = (endParentX + 1) * 2 - 1;
        int minChildY = (currentMaxY - endParentY) * 2;  // 翻转Y坐标并计算最小Y
        int maxChildY = (currentMaxY - parentY + 1) * 2 - 1;  // 翻转Y坐标并计算最大Y

        // 确保子瓦片范围不超出高层级的实际范围
        minChildX = Math.max(minChildX, highZoom.minX());
        maxChildX = Math.min(maxChildX, highZoom.maxX());
        minChildY = Math.max(minChildY, 0);
        maxChildY = Math.min(maxChildY, highMaxY);

        processTileBatch(z, minChildX, minChildY, maxChildX, maxChildY, writer);

        processedBatches++;
        if (processedBatches % 10 == 0 || processedBatches == totalBatches) {
          LOGGER.debug("缩放级别 {} 处理进度: {}/{} 批次", z, processedBatches, totalBatches);
        }
      }
    }

    long endTime = System.currentTimeMillis();
    LOGGER.info("缩放级别 {} 处理完成，耗时: {} ms", z, (endTime - startTime));
  }

  private void processTileBatch(int z, int minX, int minY, int maxX, int maxY, Mbtiles.TileWriter writer) {
    try (CloseableIterator<Tile> tileIterator = mbtiles.getZoomTiles(z + 1, minX, minY, maxX, maxY)) {
      List<CompletableFuture<Void>> futures = new ArrayList<>();
      Map<TileCoord, ConcurrentHashMap<String, ConcurrentLinkedQueue<VectorTile.Feature>>> currentZoomTiles = new ConcurrentHashMap<>();

      while (tileIterator.hasNext()) {
        Tile next = tileIterator.next();
        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> processTile(next, currentZoomTiles),
          executorService);
        futures.add(future);
      }
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
      futures.clear();

      ConcurrentLinkedQueue<TileEncodingResult> results = new ConcurrentLinkedQueue<>();
      for (Map.Entry<TileCoord, ConcurrentHashMap<String, ConcurrentLinkedQueue<VectorTile.Feature>>> entry : currentZoomTiles.entrySet()) {
        TileCoord parentTileCoord = entry.getKey();
        Map<String, ConcurrentLinkedQueue<VectorTile.Feature>> layerFeatures = entry.getValue();

        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
          VectorTile vectorTile = new VectorTile();
          try {
            for (Map.Entry<String, ConcurrentLinkedQueue<VectorTile.Feature>> layerEntry : layerFeatures.entrySet()) {
              //              handleFeatures = FeatureMerge.mergeOverlappingPolygons(idFeatures, 0);
              vectorTile.addLayerFeatures(layerEntry.getKey(), rasterizeFeatures(layerEntry.getValue(), z));
            }

            results.add(
              new TileEncodingResult(parentTileCoord, Gzip.gzip(vectorTile.encode()), OptionalLong.empty()));
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }, executorService);

        futures.add(future);
      }
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
      results.forEach(writer::write);

      futures.clear();
    } catch (Exception e) {
      LOGGER.error("处理瓦片批次 ({},{}) 到 ({},{}) 在缩放级别 {} 时发生错误",
        minX, minY, maxX, maxY, z, e);
    }
  }


  private void processTile(Tile tile,
    Map<TileCoord, ConcurrentHashMap<String, ConcurrentLinkedQueue<VectorTile.Feature>>> currentZoomTiles) {
    try {
      TileCoord parentCoord = tile.coord().parent();
      List<VectorTile.Feature> features = VectorTile.decode(Gzip.gunzip(tile.bytes()));

      for (VectorTile.Feature feature : features) {
        Geometry processedGeometry = processGeometry(feature.geometry().decode(), tile.coord());

        if (!processedGeometry.isEmpty()) {
          VectorTile.Feature copyFeature = feature.copyWithNewGeometry(VectorTile.encodeGeometry(processedGeometry));
          currentZoomTiles.computeIfAbsent(parentCoord, k -> new ConcurrentHashMap<>())
            .computeIfAbsent(feature.layer(), k -> new ConcurrentLinkedQueue<>())
            .add(copyFeature);
        } else {
          LOGGER.error("要素仿真变换后为空！");
        }
      }
    } catch (Exception e) {
      LOGGER.error("处理瓦片 {} 时发生错误", tile.coord(), e);
    }
  }

  private Geometry processGeometry(Geometry geometry, TileCoord childTile) {
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
    return geom;
  }

  private List<VectorTile.Feature> rasterizeFeatures(ConcurrentLinkedQueue<VectorTile.Feature> features, int zoom)
    throws GeometryException {
    List<VectorTile.Feature> featuresList = new ArrayList<>(features);
    if (zoom > PIXELATION_ZOOM) {
      features.clear();
      return FeatureMerge.mergeOverlappingPolygons(featuresList, 0);
    }

    ConcurrentHashMap<String, FeatureInfo> pixelFeatures = new ConcurrentHashMap<>();
    for (VectorTile.Feature feature : features) {
      pixelationGeometry(feature, pixelFeatures);
    }

    List<VectorTile.Feature> list = pixelFeatures.values().stream()
      .map(info -> info.feature).toList();
    List<VectorTile.Feature> featureList = FeatureMerge.mergeOverlappingPolygons(list, 0);
//    long end = System.currentTimeMillis();
//    LOGGER.debug("pixelFeatures time  = " + (end - start));
    features.clear();
    return featureList;
  }


  private void pixelationGeometry(VectorTile.Feature feature,
    ConcurrentHashMap<String, FeatureInfo> pixelFeatures) throws GeometryException {
    Geometry geometry = feature.geometry().decode();
    final Geometry newGeom = geometry.isValid() ? geometry : GeoUtils.fixPolygon(geometry);

    Envelope envelope = geometry.getEnvelopeInternal();
    int minX = Math.max(0, (int) (envelope.getMinX() / pixelSize));
    int minY = Math.max(0, (int) (envelope.getMinY() / pixelSize));
    int maxX = Math.min(pixelGridSize - 1, (int) Math.ceil(envelope.getMaxX() / pixelSize));
    int maxY = Math.min(pixelGridSize - 1, (int) Math.ceil(envelope.getMaxY() / pixelSize));

    for (int x = minX; x <= maxX; x++) {
      for (int y = minY; y <= maxY; y++) {
        Geometry pixelGeom = pixelGridArray[x][y];
        if (newGeom.intersects(pixelGeom)) {
          double intersectionArea = newGeom.intersection(pixelGeom).getArea();
          String pixel = x + ":" + y;
          pixelFeatures.merge(pixel,
            new FeatureInfo(feature.copyWithNewGeometry(pixelGeom), newGeom.getArea(), intersectionArea, pixel),
            (oldInfo, newInfo) -> shouldUpdateFeature(intersectionArea, newGeom, oldInfo) ? newInfo : oldInfo
          );
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

  private boolean shouldUpdateFeature(double newIntersectionArea, Geometry newGeometry, FeatureInfo oldInfo) {
    if (newIntersectionArea > oldInfo.intersectionArea) {
      return true;
    } else if (newIntersectionArea == oldInfo.intersectionArea) {
      return newGeometry.getArea() < oldInfo.area;
    }
    return false;
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
        "pixelation_grid_size_overrides", "6=512,12=256",
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
