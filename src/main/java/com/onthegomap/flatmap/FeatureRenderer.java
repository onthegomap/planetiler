package com.onthegomap.flatmap;

import com.onthegomap.flatmap.geo.GeoUtils;
import com.onthegomap.flatmap.geo.TileCoord;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FeatureRenderer {

  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureRenderer.class);
  private final CommonParams config;
  private final Consumer<RenderedFeature> consumer;

  public FeatureRenderer(CommonParams config, Consumer<RenderedFeature> consumer) {
    this.config = config;
    this.consumer = consumer;
  }

  public void renderFeature(FeatureCollector.Feature<?> feature) {
    renderGeometry(feature.getGeometry(), feature);
  }

  public void renderGeometry(Geometry geom, FeatureCollector.Feature<?> feature) {
    // TODO what about converting between area and line?
    // TODO generate ID in here?
    if (feature instanceof FeatureCollector.PointFeature pointFeature) {
      if (geom instanceof Point point) {
        addPointFeature(pointFeature, point);
      } else if (geom instanceof MultiPoint points) {
        addPointFeature(pointFeature, points);
      } else {
        LOGGER.warn("Unrecognized JTS geometry type for PointFeature:" + geom.getGeometryType());
      }
    } else if (geom instanceof Polygon || geom instanceof MultiPolygon || geom instanceof LineString
      || geom instanceof MultiLineString) {
      addLinearFeature(feature, geom);
    } else if (geom instanceof GeometryCollection collection) {
      for (int i = 0; i < collection.getNumGeometries(); i++) {
        // TODO what about feature IDs
        renderGeometry(collection.getGeometryN(i), feature);
      }
    } else {
      LOGGER.warn(
        "Unrecognized JTS geometry type for " + feature.getClass().getSimpleName() + ": " + geom.getGeometryType());
    }
  }

  private void handlePointFeature(FeatureCollector.PointFeature feature, Point point,
    Map<TileCoord, List<Point>> grouped) {

  }

  private void flushPoints(FeatureCollector.PointFeature feature, Map<TileCoord, List<Point>> grouped,
    Consumer<RenderedFeature> consumer) {
//    for (Map.Entry<TileCoord, List<Point>> entry : grouped.entrySet()) {
//      List<Point> points = entry.getValue();
//      RenderedFeature rendered = new RenderedFeature(
//        entry.getKey(),
//        new VectorTileEncoder.Feature(
//          feature.getLayer(),
//          feature.getId(),
//          VectorTileEncoder.encodeGeometry(
//            points.size() > 0 ? points.get(0) : GeoUtils.gf.createMultiPoint(points.toArray(new Point[0]))),
//          feature.getAttrsAtZoom(0)
//        ),
//        feature.getZorder(),
////        feature.ge
//      )
//    }
  }

  private static double clip(double value, double max) {
    value %= max;
    if (value < 0) {
      value += max;
    }
    return value;
  }

  private void addPointFeature(FeatureCollector.PointFeature feature, Point point) {
    for (int zoom = feature.getMaxZoom(); zoom >= feature.getMinZoom(); zoom--) {
      Map<String, Object> attrs = feature.getAttrsAtZoom(zoom);
      double buffer = feature.getBufferAtZoom(zoom);
      double tilesAtZoom = 1 << zoom;
      double worldX = point.getX() * tilesAtZoom;
      double worldY = point.getY() * tilesAtZoom;

      double labelGridTileSize = feature.getLabelGridPixelSizeAtZoom(zoom) / 256d;
      Optional<RenderedFeature.Group> groupInfo = labelGridTileSize >= 1d / 4096d ?
        Optional.of(new RenderedFeature.Group(GeoUtils.longPair(
          (int) Math.floor(clip(worldX, tilesAtZoom) / labelGridTileSize),
          (int) Math.floor(worldY / labelGridTileSize)
        ), feature.getLabelGridLimitAtZoom(zoom))) : Optional.empty();

      int minX = (int) (worldX - buffer);
      int maxX = (int) (worldX + buffer);
      int minY = (int) (worldY - buffer);
      int maxY = (int) (worldY + buffer);

      // TODO test (real, feature, unit)
      // TODO wrap x at z0
      // TODO multipoints?
      // TODO factor-out rendered feature creation
      for (int x = minX; x <= maxX; x++) {
        for (int y = minY; y <= maxY; y++) {
          TileCoord tile = TileCoord.ofXYZ(x, y, zoom);
          double tileX = worldX - x;
          double tileY = worldY - y;
          consumer.accept(new RenderedFeature(
            tile,
            new VectorTileEncoder.Feature(
              feature.getLayer(),
              feature.getId(),
              VectorTileEncoder.encodeGeometry(GeoUtils.point(tileX * 256, tileY * 256)),
              attrs
            ),
            feature.getZorder(),
            groupInfo
          ));
        }
      }
    }
  }

  private void addPointFeature(FeatureCollector.PointFeature feature, MultiPoint points) {
    Map<TileCoord, List<Point>> tilePoints = new HashMap<>();

    // TODO render features into tile
  }

  private void addLinearFeature(FeatureCollector.Feature<?> feature, Geometry geom) {
    // TODO render lines / areas into tile
  }
}
