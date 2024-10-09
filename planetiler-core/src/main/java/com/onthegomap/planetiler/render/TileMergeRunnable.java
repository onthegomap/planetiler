package com.onthegomap.planetiler.render;

import com.onthegomap.planetiler.FeatureMerge;
import com.onthegomap.planetiler.VectorTile;
import com.onthegomap.planetiler.archive.Tile;
import com.onthegomap.planetiler.archive.TileEncodingResult;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.DouglasPeuckerSimplifier;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.mbtiles.Mbtiles;
import com.onthegomap.planetiler.util.CloseableIterator;
import com.onthegomap.planetiler.util.Gzip;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.util.AffineTransformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TileMergeRunnable implements Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(TileMergeRunnable.class);

  private static final int DEFAULT_TILE_SIZE = 256;
  private static final double TILE_SCALE = 0.5d;

  private final TileCoord tileCoord;

  private final Mbtiles mbtiles;

  private final Mbtiles.TileWriter writer;

  private final PlanetilerConfig config;

  Map<String, List<AdvancedLandCoverTile.FeatureInfo>> currentZoomTiles = new ConcurrentHashMap<>();

  private final AtomicInteger count = new AtomicInteger(0);

  public TileMergeRunnable(TileCoord tileCoord, Mbtiles mbtiles, Mbtiles.TileWriter writer, PlanetilerConfig config) {
    this.tileCoord = tileCoord;
    this.mbtiles = mbtiles;
    this.writer = writer;
    this.config = config;
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
    // 1.要素合并
    try (CloseableIterator<Tile> tileIterator = mbtiles.getZoomTiles(z + 1, minChildX, minChildY, maxChildX,
      maxChildY)) {
      while (tileIterator.hasNext()) {
        Tile next = tileIterator.next();
        processTile(next);
      }

      // 2.要素简化
      List<TileEncodingResult> encodingResults = new ArrayList<>();
      for (Map.Entry<String, List<AdvancedLandCoverTile.FeatureInfo>> entry : currentZoomTiles.entrySet()) {
        encodingResults.add(encodeTile(entry.getKey(), entry.getValue(), z));
      }
      // 3.写入MBTiles
      for (TileEncodingResult encodingResult : encodingResults) {
        if (encodingResult != null) {
          writer.write(encodingResult);
        }
      }
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
  private void processTile(Tile tile) throws IOException, GeometryException {
    List<VectorTile.Feature> features = VectorTile.decode(Gzip.gunzip(tile.bytes()));
    for (VectorTile.Feature feature : features) {
      Geometry transformationGeom = simulationTransformation(feature.geometry().decode(), tile.coord());
      if (!transformationGeom.isEmpty()) {
        VectorTile.VectorGeometry vectorGeometry = VectorTile.encodeGeometry(transformationGeom, 5);
        if (vectorGeometry.isEmpty()) {
          LOGGER.warn("处理瓦片 {} 时，无法处理要素，其几何图形为空", tile.coord());
          continue;
        }

        currentZoomTiles
          .computeIfAbsent(feature.layer(), k -> new ArrayList<>())
          .add(new AdvancedLandCoverTile.FeatureInfo(feature.copyWithNewGeometry(vectorGeometry), transformationGeom,
            transformationGeom.getArea()));
        LOGGER.debug("tile:{},currentZoomTiles {}", tile, currentZoomTiles.size());
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

    Geometry geom = transform.scale(TILE_SCALE, TILE_SCALE)
      .translate(translateX, translateY)
      .transform(geometry);
    if (geom.isEmpty()) {
      LOGGER.error("仿真变换后要素为空！");
    }

    if (!geom.isValid()) {
      geom = GeoUtils.fixPolygon(geom);
    }

//    return DouglasPeuckerSimplifier.simplify(geom, 0.0625);
    return geom;
  }

  private TileEncodingResult encodeTile(String layer,
    List<AdvancedLandCoverTile.FeatureInfo> layers,
    int z) throws IOException, GeometryException {
    VectorTile vectorTile = new VectorTile();
    List<VectorTile.Feature> vectorGeometryList = rasterizeFeatures(layers, z).stream()
      .map(feature -> feature.copyWithNewGeometry(feature.geometry().unscale()))
      .toList();
    if (vectorGeometryList.isEmpty()) {
      return null;
    }
    vectorTile.addLayerFeatures(layer, vectorGeometryList);
    return new TileEncodingResult(tileCoord, Gzip.gzip(vectorTile.encode()), OptionalLong.empty());
  }

  /**
   * 要素栅格化
   *
   * @param features
   * @param zoom
   * @return
   * @throws GeometryException
   */
  public List<VectorTile.Feature> rasterizeFeatures(List<AdvancedLandCoverTile.FeatureInfo> features, int zoom)
    throws GeometryException {
    List<VectorTile.Feature> list = features.stream().map(AdvancedLandCoverTile.FeatureInfo::feature).toList();
    if (list.isEmpty()) {
      return new ArrayList<>();
    }
    return FeatureMerge.mergeOverlappingPolygons(list, 0);
  }
}
