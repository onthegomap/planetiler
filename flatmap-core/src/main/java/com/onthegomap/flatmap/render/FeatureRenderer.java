package com.onthegomap.flatmap.render;

import com.onthegomap.flatmap.FeatureCollector;
import com.onthegomap.flatmap.VectorTile;
import com.onthegomap.flatmap.config.FlatmapConfig;
import com.onthegomap.flatmap.geo.DouglasPeuckerSimplifier;
import com.onthegomap.flatmap.geo.GeoUtils;
import com.onthegomap.flatmap.geo.GeometryException;
import com.onthegomap.flatmap.geo.TileCoord;
import com.onthegomap.flatmap.geo.TileExtents;
import com.onthegomap.flatmap.stats.Stats;
import java.util.HashMap;
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
import org.locationtech.jts.geom.impl.PackedCoordinateSequence;
import org.locationtech.jts.geom.util.AffineTransformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Converts source features geometries to encoded vector tile features according to settings configured in the map
 * profile (like zoom range, min pixel size, output attributes and their zoom ranges).
 */
public class FeatureRenderer implements Consumer<FeatureCollector.Feature> {

  // generate globally-unique IDs shared by all vector tile features representing the same source feature
  private static final AtomicLong idGenerator = new AtomicLong(0);
  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureRenderer.class);
  private static final VectorTile.VectorGeometry FILL = VectorTile.encodeGeometry(GeoUtils.JTS_FACTORY
    .createPolygon(GeoUtils.JTS_FACTORY.createLinearRing(new PackedCoordinateSequence.Double(new double[]{
      -5, -5,
      261, -5,
      261, 261,
      -5, 261,
      -5, -5
    }, 2, 0))));
  private final FlatmapConfig config;
  private final Consumer<RenderedFeature> consumer;
  private final Stats stats;

  /** Constructs a new feature render that will send rendered features to {@code consumer}. */
  public FeatureRenderer(FlatmapConfig config, Consumer<RenderedFeature> consumer, Stats stats) {
    this.config = config;
    this.consumer = consumer;
    this.stats = stats;
  }

  @Override
  public void accept(FeatureCollector.Feature feature) {
    renderGeometry(feature.getGeometry(), feature);
  }

  private void renderGeometry(Geometry geom, FeatureCollector.Feature feature) {
    if (geom.isEmpty()) {
      LOGGER.warn("Empty geometry " + feature);
    } else if (geom instanceof Point point) {
      renderPoint(feature, point.getCoordinates());
    } else if (geom instanceof MultiPoint points) {
      renderPoint(feature, points);
    } else if (geom instanceof Polygon || geom instanceof MultiPolygon || geom instanceof LineString
      || geom instanceof MultiLineString) {
      renderLineOrPolygon(feature, geom);
    } else if (geom instanceof GeometryCollection collection) {
      for (int i = 0; i < collection.getNumGeometries(); i++) {
        renderGeometry(collection.getGeometryN(i), feature);
      }
    } else {
      LOGGER.warn(
        "Unrecognized JTS geometry type for " + feature.getClass().getSimpleName() + ": " + geom.getGeometryType());
    }
  }

  private void renderPoint(FeatureCollector.Feature feature, Coordinate... coords) {
    long id = idGenerator.incrementAndGet();
    boolean hasLabelGrid = feature.hasLabelGrid();
    for (int zoom = feature.getMaxZoom(); zoom >= feature.getMinZoom(); zoom--) {
      Map<String, Object> attrs = feature.getAttrsAtZoom(zoom);
      double buffer = feature.getBufferPixelsAtZoom(zoom) / 256;
      int tilesAtZoom = 1 << zoom;

      // for "label grid" point density limiting, compute the grid square that this point sits in
      // only valid if not a multipoint
      RenderedFeature.Group groupInfo = null;
      if (hasLabelGrid && coords.length == 1) {
        double labelGridTileSize = feature.getPointLabelGridPixelSizeAtZoom(zoom) / 256d;
        groupInfo = labelGridTileSize < 1d / 4096d ? null : new RenderedFeature.Group(
          GeoUtils.labelGridId(tilesAtZoom, labelGridTileSize, coords[0]),
          feature.getPointLabelGridLimitAtZoom(zoom)
        );
      }

      // compute the tile coordinate of every tile these points should show up in at the given buffer size
      TileExtents.ForZoom extents = config.bounds().tileExtents().getForZoom(zoom);
      TiledGeometry tiled = TiledGeometry.slicePointsIntoTiles(extents, buffer, zoom, coords, feature.getSourceId());
      int emitted = 0;
      for (var entry : tiled.getTileData()) {
        TileCoord tile = entry.getKey();
        List<List<CoordinateSequence>> result = entry.getValue();
        Geometry geom = GeometryCoordinateSequences.reassemblePoints(result);
        encodeAndEmitFeature(feature, id, attrs, tile, geom, groupInfo);
        emitted++;
      }
      stats.emittedFeatures(zoom, feature.getLayer(), emitted);
    }

    stats.processedElement("point", feature.getLayer());
  }

  private void encodeAndEmitFeature(FeatureCollector.Feature feature, long id, Map<String, Object> attrs,
    TileCoord tile, Geometry geom, RenderedFeature.Group groupInfo) {
    consumer.accept(new RenderedFeature(
      tile,
      new VectorTile.Feature(
        feature.getLayer(),
        id,
        VectorTile.encodeGeometry(geom),
        attrs,
        groupInfo == null ? VectorTile.Feature.NO_GROUP : groupInfo.group()
      ),
      feature.getZorder(),
      Optional.ofNullable(groupInfo)
    ));
  }

  private void renderPoint(FeatureCollector.Feature feature, MultiPoint points) {
    /*
     * Attempt to encode multipoints as a single feature sharing attributes and z-order
     * but if it has label grid data then need to fall back to separate features per point,
     * so they can be filtered individually.
     */
    if (feature.hasLabelGrid()) {
      for (Coordinate coord : points.getCoordinates()) {
        renderPoint(feature, coord);
      }
    } else {
      renderPoint(feature, points.getCoordinates());
    }
  }

  private void renderLineOrPolygon(FeatureCollector.Feature feature, Geometry input) {
    long id = idGenerator.incrementAndGet();
    boolean area = input instanceof Polygonal;
    double worldLength = (area || input.getNumGeometries() > 1) ? 0 : input.getLength();
    String numPointsAttr = feature.getNumPointsAttr();
    for (int z = feature.getMaxZoom(); z >= feature.getMinZoom(); z--) {
      double scale = 1 << z;
      double tolerance = feature.getPixelToleranceAtZoom(z) / 256d;
      double minSize = feature.getMinPixelSizeAtZoom(z) / 256d;
      if (area) {
        // treat minPixelSize as the edge of a square that defines minimum area for features
        minSize *= minSize;
      } else if (worldLength > 0 && worldLength * scale < minSize) {
        // skip linestring, too short
        continue;
      }

      // TODO potential optimization: iteratively simplify z+1 to get z instead of starting with original geom each time
      // simplify only takes 4-5 minutes of wall time when generating the planet though, so not a big deal
      Geometry geom = AffineTransformation.scaleInstance(scale, scale).transform(input);
      geom = DouglasPeuckerSimplifier.simplify(geom, tolerance);

      List<List<CoordinateSequence>> groups = GeometryCoordinateSequences.extractGroups(geom, minSize);
      double buffer = feature.getBufferPixelsAtZoom(z) / 256;
      TileExtents.ForZoom extents = config.bounds().tileExtents().getForZoom(z);
      TiledGeometry sliced = TiledGeometry.sliceIntoTiles(groups, buffer, area, z, extents, feature.getSourceId());
      Map<String, Object> attrs = feature.getAttrsAtZoom(sliced.zoomLevel());
      if (numPointsAttr != null) {
        // if profile wants the original number of points that the simplified but untiled geometry started with
        attrs = new HashMap<>(attrs);
        attrs.put(numPointsAttr, geom.getNumPoints());
      }
      writeTileFeatures(z, id, feature, sliced, attrs);
    }

    stats.processedElement(area ? "polygon" : "line", feature.getLayer());
  }

  private void writeTileFeatures(int zoom, long id, FeatureCollector.Feature feature, TiledGeometry sliced,
    Map<String, Object> attrs) {
    int emitted = 0;
    for (var entry : sliced.getTileData()) {
      TileCoord tile = entry.getKey();
      try {
        List<List<CoordinateSequence>> geoms = entry.getValue();

        Geometry geom;
        if (feature.isPolygon()) {
          geom = GeometryCoordinateSequences.reassemblePolygons(geoms);
          /*
           * Use the very expensive, but necessary JTS Geometry#buffer(0) trick to repair invalid polygons (with self-
           * intersections) and JTS GeometryPrecisionReducer utility to snap polygon nodes to the vector tile grid
           * without introducing self-intersections.
           *
           * See https://docs.mapbox.com/vector-tiles/specification/#simplification for issues that can arise from naive
           * coordinate rounding.
           */
          geom = GeoUtils.snapAndFixPolygon(geom);
          // JTS utilities "fix" the geometry to be clockwise outer/CCW inner but vector tiles flip Y coordinate,
          // so we need outer CCW/inner clockwise
          geom = geom.reverse();
        } else {
          geom = GeometryCoordinateSequences.reassembleLineStrings(geoms);
        }

        if (!geom.isEmpty()) {
          encodeAndEmitFeature(feature, id, attrs, tile, geom, null);
          emitted++;
        }
      } catch (GeometryException e) {
        e.log(stats, "write_tile_features", "Error writing tile " + tile + " feature " + feature);
      }
    }

    // polygons that span multiple tiles contain detail about the outer edges separate from the filled tiles, so emit
    // filled tiles now
    if (feature.isPolygon()) {
      emitted += emitFilledTiles(id, feature, sliced);
    }

    stats.emittedFeatures(zoom, feature.getLayer(), emitted);
  }

  private int emitFilledTiles(long id, FeatureCollector.Feature feature, TiledGeometry sliced) {
    Optional<RenderedFeature.Group> groupInfo = Optional.empty();
    /*
     * Optimization: large input polygons that generate many filled interior tiles (i.e. the ocean), the encoder avoids
     * re-encoding if groupInfo and vector tile feature are == to previous values, so compute one instance at the start
     * of each zoom level for this feature.
     */
    VectorTile.Feature vectorTileFeature = new VectorTile.Feature(
      feature.getLayer(),
      id,
      FILL,
      feature.getAttrsAtZoom(sliced.zoomLevel())
    );

    int emitted = 0;
    for (TileCoord tile : sliced.getFilledTiles()) {
      consumer.accept(new RenderedFeature(
        tile,
        vectorTileFeature,
        feature.getZorder(),
        groupInfo
      ));
      emitted++;
    }
    return emitted;
  }
}
