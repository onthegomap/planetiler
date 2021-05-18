package com.onthegomap.flatmap;

import com.onthegomap.flatmap.geo.GeoUtils;
import com.onthegomap.flatmap.geo.TileCoord;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateXY;
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

  private static final AtomicLong idGen = new AtomicLong(0);

  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureRenderer.class);
  private final CommonParams config;
  private final Consumer<RenderedFeature> consumer;

  public FeatureRenderer(CommonParams config, Consumer<RenderedFeature> consumer) {
    this.config = config;
    this.consumer = consumer;
  }

  public void renderFeature(FeatureCollector.Feature feature) {
    renderGeometry(feature.getGeometry(), feature);
  }

  private void renderGeometry(Geometry geom, FeatureCollector.Feature feature) {
    // TODO what about converting between area and line?
    if (geom.isEmpty()) {
      LOGGER.warn("Empty geometry " + feature);
    } else if (geom instanceof Point point) {
      addPointFeature(feature, point.getCoordinates());
    } else if (geom instanceof MultiPoint points) {
      addPointFeature(feature, points);
    } else if (geom instanceof Polygon || geom instanceof MultiPolygon || geom instanceof LineString
      || geom instanceof MultiLineString) {
      addLinearFeature(feature, geom);
    } else if (geom instanceof GeometryCollection collection) {
      for (int i = 0; i < collection.getNumGeometries(); i++) {
        renderGeometry(collection.getGeometryN(i), feature);
      }
    } else {
      LOGGER.warn(
        "Unrecognized JTS geometry type for " + feature.getClass().getSimpleName() + ": " + geom.getGeometryType());
    }
  }

  private static int wrapInt(int value, int max) {
    value %= max;
    if (value < 0) {
      value += max;
    }
    return value;
  }

  private static double wrapDouble(double value, double max) {
    value %= max;
    if (value < 0) {
      value += max;
    }
    return value;
  }

  private void slicePoint(Map<TileCoord, Set<Coordinate>> output, int zoom, double buffer, Coordinate coord) {
    int tilesAtZoom = 1 << zoom;
    double worldX = coord.getX() * tilesAtZoom;
    double worldY = coord.getY() * tilesAtZoom;
    int minX = (int) Math.floor(worldX - buffer);
    int maxX = (int) Math.floor(worldX + buffer);
    int minY = Math.max(0, (int) Math.floor(worldY - buffer));
    int maxY = Math.min(tilesAtZoom - 1, (int) Math.floor(worldY + buffer));
    for (int x = minX; x <= maxX; x++) {
      double tileX = worldX - x;
      for (int y = minY; y <= maxY; y++) {
        TileCoord tile = TileCoord.ofXYZ(wrapInt(x, tilesAtZoom), y, zoom);
        double tileY = worldY - y;
        Coordinate outCoordinate = new CoordinateXY(tileX * 256, tileY * 256);
        output.computeIfAbsent(tile, t -> new HashSet<>()).add(outCoordinate);
      }
    }
  }

  private void addPointFeature(FeatureCollector.Feature feature, Coordinate... coords) {
    long id = idGen.incrementAndGet();
    for (int zoom = feature.getMaxZoom(); zoom >= feature.getMinZoom(); zoom--) {
      Map<TileCoord, Set<Coordinate>> sliced = new HashMap<>();
      Map<String, Object> attrs = feature.getAttrsAtZoom(zoom);
      double buffer = feature.getBufferPixelsAtZoom(zoom) / 256;
      int tilesAtZoom = 1 << zoom;
      for (Coordinate coord : coords) {
        slicePoint(sliced, zoom, buffer, coord);
      }

      Optional<RenderedFeature.Group> groupInfo = Optional.empty();
      if (feature.hasLabelGrid() && coords.length == 1) {
        double labelGridTileSize = feature.getLabelGridPixelSizeAtZoom(zoom) / 256d;
        groupInfo = labelGridTileSize >= 1d / 4096d ?
          Optional.of(new RenderedFeature.Group(GeoUtils.longPair(
            (int) Math.floor(wrapDouble(coords[0].getX() * tilesAtZoom, tilesAtZoom) / labelGridTileSize),
            (int) Math.floor((coords[0].getY() * tilesAtZoom) / labelGridTileSize)
          ), feature.getLabelGridLimitAtZoom(zoom))) : Optional.empty();
      }

      for (var entry : sliced.entrySet()) {
        Set<Coordinate> value = entry.getValue();
        Geometry geom = value.size() == 1 ? GeoUtils.point(value.iterator().next()) : GeoUtils.multiPoint(value);
        consumer.accept(new RenderedFeature(
          entry.getKey(),
          new VectorTileEncoder.Feature(
            feature.getLayer(),
            id,
            VectorTileEncoder.encodeGeometry(geom),
            attrs
          ),
          feature.getZorder(),
          groupInfo
        ));
      }
    }
  }

  private void addPointFeature(FeatureCollector.Feature feature, MultiPoint points) {
    if (feature.hasLabelGrid()) {
      for (Coordinate coord : points.getCoordinates()) {
        addPointFeature(feature, coord);
      }
    } else {
      addPointFeature(feature, points.getCoordinates());
    }
  }

  private void addLinearFeature(FeatureCollector.Feature feature, Geometry geom) {
    double minSizeAtMaxZoom = 1d / 4096;
    double normalTolerance = 0.1 / 256;
    double toleranceAtMaxZoom = 1d / 4096;


  }

}
