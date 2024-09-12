package com.onthegomap.planetiler.render;

import com.onthegomap.planetiler.FeatureMerge;
import com.onthegomap.planetiler.VectorTile;
import com.onthegomap.planetiler.archive.Tile;
import com.onthegomap.planetiler.archive.TileArchiveMetadata;
import com.onthegomap.planetiler.archive.TileArchives;
import com.onthegomap.planetiler.archive.TileEncodingResult;
import com.onthegomap.planetiler.archive.WriteableTileArchive;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.mbtiles.Mbtiles;
import com.onthegomap.planetiler.util.CloseableIterator;
import com.onthegomap.planetiler.util.Gzip;
import com.onthegomap.planetiler.util.JsonUitls;
import com.onthegomap.planetiler.util.ZoomFunction;
import java.io.IOException;
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

  public void generateLowerZoomTiles() throws GeometryException, IOException {
    // TODO 现获取矢量数据的范围，根据范围计算底层级的瓦片后再去获取高层层级瓦片，避免在大数据量情况下获取全部高层级的瓦片把内存撑爆
    try (Mbtiles.TileWriter writer = mbtiles.newTileWriter()) {
      for (int zoom = config.maxzoom(); zoom > config.minzoom(); zoom--) {
        if (zoom == config.maxzoom()) {
          writer.setTileDataIdCounter(mbtiles.getMaxDataTileId() + 1);
        }

        // 根据层级提前初始化PIXEL_GRID和PIXEL_SIZE
        initPixelSize(zoom - 1);
        processZoomLevel(zoom - 1, writer);
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

  public void processZoomLevel(int zoom, Mbtiles.TileWriter writer) {
    LOGGER.info("开始处理缩放级别 {}", zoom);
    long startTime = System.currentTimeMillis();

    ConcurrentLinkedQueue<TileEncodingResult> results = new ConcurrentLinkedQueue<>();
    try (CloseableIterator<Tile> tileIterator = mbtiles.getZoomTiles(zoom + 1)) {
      Map<TileCoord, ConcurrentHashMap<String, ConcurrentLinkedQueue<VectorTile.Feature>>> currentZoomTiles = new ConcurrentHashMap<>();

      // 并行处理所有瓦片，仿真变换
      List<CompletableFuture<Void>> futures = new ArrayList<>();
      while (tileIterator.hasNext()) {
        Tile next = tileIterator.next();
        CompletableFuture<Void> future = CompletableFuture.runAsync(
          () -> processTile(next, currentZoomTiles), executorService);
        futures.add(future);
      }
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

      futures.clear();

      for (Map.Entry<TileCoord, ConcurrentHashMap<String, ConcurrentLinkedQueue<VectorTile.Feature>>> entry : currentZoomTiles.entrySet()) {
        TileCoord parentTileCoord = entry.getKey();
        Map<String, ConcurrentLinkedQueue<VectorTile.Feature>> layerFeatures = entry.getValue();

        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
          VectorTile vectorTile = new VectorTile();
          for (Map.Entry<String, ConcurrentLinkedQueue<VectorTile.Feature>> layerEntry : layerFeatures.entrySet()) {
            List<VectorTile.Feature> handleFeatures;
            try {
//              handleFeatures = FeatureMerge.mergeOverlappingPolygons(idFeatures, 0);
              handleFeatures = rasterizeFeatures(layerEntry.getValue(), zoom);
            } catch (GeometryException e) {
              throw new RuntimeException(e);
            }
            vectorTile.addLayerFeatures(layerEntry.getKey(), handleFeatures);
          }

          try {
            results.add(new TileEncodingResult(parentTileCoord, Gzip.gzip(vectorTile.encode()), OptionalLong.empty()));
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }, executorService);

        futures.add(future);
      }
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

      results.forEach(writer::write);
    }

    long endTime = System.currentTimeMillis();
    LOGGER.info("缩放级别 {} 处理完成，耗时: {} ms", zoom, (endTime - startTime));
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
    String mbtilesPath = "E:\\Linespace\\SceneMapServer\\Data\\parquet\\guangdong-latest.osm.pbf\\default-14\\default-14 - 副本.mbtiles";
    PlanetilerConfig planetilerConfig = PlanetilerConfig.from(
      Arguments.of("minzoom", 0, "maxzoom", 14, "pixelation_grid_size_overrides", "6=512,12=256"));
//"pixel_size_overrides", "12=128"
    try (WriteableTileArchive archive = TileArchives.newWriter(Paths.get(mbtilesPath), planetilerConfig)) {
      AdvancedLandCoverTiling tiling = new AdvancedLandCoverTiling(planetilerConfig, (Mbtiles) archive);

      tiling.generateLowerZoomTiles();
      LOGGER.info("所有缩放级别处理完成");
    } catch (Exception e) {
      LOGGER.error("处理过程中发生错误", e);
    }
  }

}
