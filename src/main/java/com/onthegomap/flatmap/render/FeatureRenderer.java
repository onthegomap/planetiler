package com.onthegomap.flatmap.render;

import com.onthegomap.flatmap.CommonParams;
import com.onthegomap.flatmap.FeatureCollector;
import com.onthegomap.flatmap.TileExtents;
import com.onthegomap.flatmap.VectorTileEncoder;
import com.onthegomap.flatmap.geo.GeoUtils;
import com.onthegomap.flatmap.geo.GeometryException;
import com.onthegomap.flatmap.geo.TileCoord;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.Polygonal;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.geom.impl.PackedCoordinateSequence;
import org.locationtech.jts.geom.util.AffineTransformation;
import org.locationtech.jts.precision.GeometryPrecisionReducer;
import org.locationtech.jts.simplify.DouglasPeuckerSimplifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FeatureRenderer {

  private static final AtomicLong idGen = new AtomicLong(0);

  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureRenderer.class);
  private static final PrecisionModel tilePrecision = new PrecisionModel(4096d / 256d);
  private static final VectorTileEncoder.VectorGeometry FILL = VectorTileEncoder.encodeGeometry(GeoUtils.JTS_FACTORY
    .createPolygon(GeoUtils.JTS_FACTORY.createLinearRing(new PackedCoordinateSequence.Double(new double[]{
      -5, -5,
      261, -5,
      261, 261,
      -5, 261,
      -5, -5
    }, 2, 0))));
  private final CommonParams config;
  private final Consumer<RenderedFeature> consumer;

  public FeatureRenderer(CommonParams config, Consumer<RenderedFeature> consumer) {
    this.config = config;
    this.consumer = consumer;
  }

  private static int wrapInt(int value, int max) {
    value %= max;
    if (value < 0) {
      value += max;
    }
    return value;
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

  private void slicePoint(Map<TileCoord, Set<Coordinate>> output, int zoom, double buffer, Coordinate coord) {
    // TODO put this into TiledGeometry
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
        tilePrecision.makePrecise(outCoordinate);
        output.computeIfAbsent(tile, t -> new HashSet<>()).add(outCoordinate);
      }
    }
  }

  private void addPointFeature(FeatureCollector.Feature feature, Coordinate... coords) {
    long id = idGen.incrementAndGet();
    boolean hasLabelGrid = feature.hasLabelGrid();
    for (int zoom = feature.getMaxZoom(); zoom >= feature.getMinZoom(); zoom--) {
      Map<TileCoord, Set<Coordinate>> sliced = new HashMap<>();
      Map<String, Object> attrs = feature.getAttrsAtZoom(zoom);
      double buffer = feature.getBufferPixelsAtZoom(zoom) / 256;
      int tilesAtZoom = 1 << zoom;
      for (Coordinate coord : coords) {
        // TODO TiledGeometry.sliceIntoTiles(...)
        slicePoint(sliced, zoom, buffer, coord);
      }

      RenderedFeature.Group groupInfo = null;
      if (hasLabelGrid && coords.length == 1) {
        double labelGridTileSize = feature.getLabelGridPixelSizeAtZoom(zoom) / 256d;
        groupInfo = labelGridTileSize < 1d / 4096d ? null : new RenderedFeature.Group(
          GeoUtils.labelGridId(tilesAtZoom, labelGridTileSize, coords[0]),
          feature.getLabelGridLimitAtZoom(zoom)
        );
      }

      for (var entry : sliced.entrySet()) {
        TileCoord tile = entry.getKey();
        Set<Coordinate> value = entry.getValue();
        Geometry geom = value.size() == 1 ? GeoUtils.point(value.iterator().next()) : GeoUtils.multiPoint(value);
        // TODO stats
        // TODO writeTileFeatures
        emitFeature(feature, id, attrs, tile, geom, groupInfo);
      }
    }
  }

  private void emitFeature(FeatureCollector.Feature feature, long id, Map<String, Object> attrs, TileCoord tile,
    Geometry geom, RenderedFeature.Group groupInfo) {
    consumer.accept(new RenderedFeature(
      tile,
      new VectorTileEncoder.Feature(
        feature.getLayer(),
        id,
        VectorTileEncoder.encodeGeometry(geom),
        attrs
      ),
      feature.getZorder(),
      Optional.ofNullable(groupInfo)
    ));
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

  private void addLinearFeature(FeatureCollector.Feature feature, Geometry input) {
    long id = idGen.incrementAndGet();
    // TODO move to feature?
    double minSizeAtMaxZoom = 1d / 4096;
    double normalTolerance = 0.1 / 256;
    double toleranceAtMaxZoom = 1d / 4096;

    boolean area = input instanceof Polygonal;
    double worldLength = (area || input.getNumGeometries() > 1) ? 0 : input.getLength();
    for (int z = feature.getMaxZoom(); z >= feature.getMinZoom(); z--) {
      boolean isMaxZoom = feature.getMaxZoom() == 14;
      double scale = 1 << z;
      double tolerance = isMaxZoom ? toleranceAtMaxZoom : normalTolerance;
      double minSize = isMaxZoom ? minSizeAtMaxZoom : (feature.getMinPixelSize(z) / 256);
      if (area) {
        minSize *= minSize;
      } else if (worldLength > 0 && worldLength * scale < minSize) {
        // skip linestring, too short
        continue;
      }

      Geometry geom = AffineTransformation.scaleInstance(scale, scale).transform(input);
      DouglasPeuckerSimplifier simplifier = new DouglasPeuckerSimplifier(geom);
      simplifier.setEnsureValid(false);
      simplifier.setDistanceTolerance(tolerance);
      geom = simplifier.getResultGeometry();

      List<List<CoordinateSequence>> groups = CoordinateSequenceExtractor.extractGroups(geom, minSize);
      double buffer = feature.getBufferPixelsAtZoom(z);
      TileExtents.ForZoom extents = config.extents().getForZoom(z);
      TiledGeometry sliced = TiledGeometry.sliceIntoTiles(groups, buffer, area, z, extents);
      writeTileFeatures(id, feature, sliced);
    }
  }

  private void writeTileFeatures(long id, FeatureCollector.Feature feature, TiledGeometry sliced) {
    Map<String, Object> attrs = feature.getAttrsAtZoom(sliced.zoomLevel());
    for (var entry : sliced.getTileData()) {
      TileCoord tile = entry.getKey();
      try {
        List<List<CoordinateSequence>> geoms = entry.getValue();

        Geometry geom;
        if (feature.area()) {
          geom = CoordinateSequenceExtractor.reassemblePolygon(feature, tile, geoms);
          geom = GeoUtils.fixPolygon(geom, 2);
        } else {
          geom = CoordinateSequenceExtractor.reassembleLineString(geoms);
        }

        try {
          geom = GeometryPrecisionReducer.reduce(geom, tilePrecision);
        } catch (IllegalArgumentException e) {
          throw new GeometryException("Error reducing precision");
        }

        if (!geom.isEmpty()) {
          // JTS utilities "fix" the geometry to be clockwise outer/CCW inner
          if (feature.area()) {
            geom = geom.reverse();
          }
          emitFeature(feature, id, attrs, tile, geom, null);
        }
      } catch (GeometryException e) {
        LOGGER.warn(e.getMessage() + ": " + tile + " " + feature);
      }
    }

    if (feature.area()) {
      emitFilledTiles(id, feature, sliced);
    }
    // TODO log stats
  }

  private void emitFilledTiles(long id, FeatureCollector.Feature feature, TiledGeometry sliced) {
    /*
     * Optimization: large input polygons that generate many filled interior tiles (ie. the ocean), the encoder avoids
     * re-encoding if groupInfo and vector tile feature are == to previous values.
     */
    Optional<RenderedFeature.Group> groupInfo = Optional.empty();
    VectorTileEncoder.Feature vectorTileFeature = new VectorTileEncoder.Feature(
      feature.getLayer(),
      id,
      FILL,
      feature.getAttrsAtZoom(sliced.zoomLevel())
    );

    for (TileCoord tile : sliced.getFilledTiles()) {
      consumer.accept(new RenderedFeature(
        tile,
        vectorTileFeature,
        feature.getZorder(),
        groupInfo
      ));
    }
  }
}
