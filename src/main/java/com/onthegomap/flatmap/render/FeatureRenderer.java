package com.onthegomap.flatmap.render;

import com.onthegomap.flatmap.CommonParams;
import com.onthegomap.flatmap.FeatureCollector;
import com.onthegomap.flatmap.TileExtents;
import com.onthegomap.flatmap.VectorTileEncoder;
import com.onthegomap.flatmap.geo.GeoUtils;
import com.onthegomap.flatmap.geo.GeometryException;
import com.onthegomap.flatmap.geo.TileCoord;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
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

  private void addPointFeature(FeatureCollector.Feature feature, Coordinate... coords) {
    long id = idGen.incrementAndGet();
    boolean hasLabelGrid = feature.hasLabelGrid();
    for (int zoom = feature.getMaxZoom(); zoom >= feature.getMinZoom(); zoom--) {
      Map<String, Object> attrs = feature.getAttrsAtZoom(zoom);
      double buffer = feature.getBufferPixelsAtZoom(zoom) / 256;
      int tilesAtZoom = 1 << zoom;
      TileExtents.ForZoom extents = config.extents().getForZoom(zoom);
      TiledGeometry tiled = TiledGeometry.slicePointsIntoTiles(extents, buffer, zoom, coords);

      RenderedFeature.Group groupInfo = null;
      if (hasLabelGrid && coords.length == 1) {
        double labelGridTileSize = feature.getLabelGridPixelSizeAtZoom(zoom) / 256d;
        groupInfo = labelGridTileSize < 1d / 4096d ? null : new RenderedFeature.Group(
          GeoUtils.labelGridId(tilesAtZoom, labelGridTileSize, coords[0]),
          feature.getLabelGridLimitAtZoom(zoom)
        );
      }

      for (var entry : tiled.getTileData()) {
        TileCoord tile = entry.getKey();
        List<List<CoordinateSequence>> result = entry.getValue();
        Geometry geom = CoordinateSequenceExtractor.reassemblePoints(result);
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
    boolean area = input instanceof Polygonal;
    double worldLength = (area || input.getNumGeometries() > 1) ? 0 : input.getLength();
    for (int z = feature.getMaxZoom(); z >= feature.getMinZoom(); z--) {
      double scale = 1 << z;
      double tolerance = feature.getPixelTolerance(z) / 256d;
      double minSize = feature.getMinPixelSize(z) / 256d;
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
      double buffer = feature.getBufferPixelsAtZoom(z) / 256;
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
          geom = CoordinateSequenceExtractor.reassemblePolygons(geoms);
          geom = GeoUtils.snapAndFixPolygon(geom, tilePrecision);
          // JTS utilities "fix" the geometry to be clockwise outer/CCW inner
          geom = geom.reverse();
        } else {
          geom = CoordinateSequenceExtractor.reassembleLineStrings(geoms);
        }

        if (!geom.isEmpty()) {
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
