package com.onthegomap.planetiler.render;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.VectorTile;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.DouglasPeuckerSimplifier;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.geo.GeometryPipeline;
import com.onthegomap.planetiler.geo.SimplifyMethod;
import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.geo.TileExtents;
import com.onthegomap.planetiler.geo.VWSimplifier;
import com.onthegomap.planetiler.stats.Stats;
import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Lineal;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.Polygonal;
import org.locationtech.jts.geom.Puntal;
import org.locationtech.jts.geom.util.AffineTransformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Converts source features geometries to encoded vector tile features according to settings configured in the map
 * profile (like zoom range, min pixel size, output attributes and their zoom ranges).
 */
public class FeatureRenderer implements Consumer<FeatureCollector.Feature>, Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureRenderer.class);
  private final PlanetilerConfig config;
  private final Consumer<RenderedFeature> consumer;
  private final Stats stats;
  private final Closeable closeable;

  /** Constructs a new feature render that will send rendered features to {@code consumer}. */
  public FeatureRenderer(PlanetilerConfig config, Consumer<RenderedFeature> consumer, Stats stats,
    Closeable closeable) {
    this.config = config;
    this.consumer = consumer;
    this.stats = stats;
    this.closeable = closeable;
  }

  public FeatureRenderer(PlanetilerConfig config, Consumer<RenderedFeature> consumer, Stats stats) {
    this(config, consumer, stats, null);
  }

  @Override
  public void accept(FeatureCollector.Feature feature) {
    var geometry = feature.getGeometry();
    double simpleLineLength =
      geometry instanceof Lineal && geometry.getNumGeometries() == 1 ? geometry.getLength() : -1;
    if (geometry.isEmpty()) {
      LOGGER.warn("Empty geometry {}", feature);
      return;
    }
    // geometries are filtered by min size after processing before they are emitted, but do cheap pre-filtering here
    // to avoid processing features that won't emit anything
    for (int zoom = feature.getMaxZoom(); zoom >= feature.getMinZoom(); zoom--) {
      double scale = 1 << zoom;
      double minSize = feature.getMinPixelSizeAtZoom(zoom);
      if (feature.hasLinearRanges()) {
        double length = simpleLineLength * scale * 256;
        for (var range : feature.getLinearRangesAtZoom(zoom)) {
          if (minSize > 0 && length * (range.end() - range.start()) > minSize) {
            accept(zoom, range.geom(), range.attrs(), feature);
          }
        }
      } else {
        if (minSize > 0) {
          if (geometry instanceof Puntal) {
            if (!feature.source().isPoint() && feature.getSourceFeaturePixelSizeAtZoom(zoom) < minSize) {
              // don't emit points if the line or polygon feature it came from was too small
              continue;
            }
          } else if (simpleLineLength >= 0 && simpleLineLength * scale * 256 < minSize) {
            // skip processing lines that are too short
            continue;
          }
        }
        accept(zoom, geometry, feature.getAttrsAtZoom(zoom), feature);
      }
    }
  }

  private void accept(int zoom, Geometry geom, Map<String, Object> attrs, FeatureCollector.Feature feature) {
    double scale = 1 << zoom;
    geom = AffineTransformation.scaleInstance(scale, scale).transform(geom);
    GeometryPipeline pipeline = feature.getGeometryPipelineAtZoom(zoom);
    if (pipeline != null) {
      geom = pipeline.apply(geom);
    } else if (!(geom instanceof Puntal)) {
      geom = simplify(zoom, geom, feature);
    }

    renderGeometry(zoom, geom, attrs, feature);
  }

  private static Geometry simplify(int zoom, Geometry scaled, FeatureCollector.Feature feature) {
    double tolerance = feature.getPixelToleranceAtZoom(zoom) / 256d;
    SimplifyMethod simplifyMethod = feature.getSimplifyMethodAtZoom(zoom);
    scaled = switch (simplifyMethod) {
      case RETAIN_IMPORTANT_POINTS -> DouglasPeuckerSimplifier.simplify(scaled, tolerance);
      // DP tolerance is displacement, and VW tolerance is area, so square what the user entered to convert from
      // DP to VW tolerance
      case RETAIN_EFFECTIVE_AREAS -> new VWSimplifier().setTolerance(tolerance * tolerance).transform(scaled);
      case RETAIN_WEIGHTED_EFFECTIVE_AREAS ->
        new VWSimplifier().setWeight(0.7).setTolerance(tolerance * tolerance).transform(scaled);
    };
    return scaled;
  }

  private void renderGeometry(int zoom, Geometry geom, Map<String, Object> attrs, FeatureCollector.Feature feature) {
    if (geom == null || geom.isEmpty()) {
      // skip this feature
    } else if (geom instanceof Point point) {
      renderPoint(zoom, attrs, feature, point.getCoordinates());
    } else if (geom instanceof MultiPoint points) {
      renderPoint(zoom, attrs, feature, points);
    } else if (geom instanceof Polygon || geom instanceof MultiPolygon || geom instanceof LineString ||
      geom instanceof MultiLineString) {
      renderLineOrPolygon(zoom, attrs, feature, geom);
    } else if (geom instanceof GeometryCollection collection) {
      for (int i = 0; i < collection.getNumGeometries(); i++) {
        renderGeometry(zoom, collection.getGeometryN(i), attrs, feature);
      }
    } else {
      LOGGER.warn("Unrecognized JTS geometry type for {}: {}", feature.getClass().getSimpleName(),
        geom.getGeometryType());
    }
  }

  private void renderPoint(int zoom, Map<String, Object> attrs, FeatureCollector.Feature feature,
    Coordinate... coords) {
    boolean hasLabelGrid = feature.hasLabelGrid();
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
    TiledGeometry tiled = TiledGeometry.slicePointsIntoTiles(extents, buffer, zoom, coords);
    int emitted = 0;
    for (var entry : tiled.getTileData().entrySet()) {
      TileCoord tile = entry.getKey();
      List<List<CoordinateSequence>> result = entry.getValue();
      Geometry geom = GeometryCoordinateSequences.reassemblePoints(result);
      encodeAndEmitFeature(feature, feature.getId(), attrs, tile, geom, groupInfo, 0);
      emitted++;
    }
    stats.emittedFeatures(zoom, feature.getLayer(), emitted);

    stats.processedElement("point", feature.getLayer(), zoom);
  }

  private void encodeAndEmitFeature(FeatureCollector.Feature feature, long id, Map<String, Object> attrs,
    TileCoord tile, Geometry geom, RenderedFeature.Group groupInfo, int scale) {
    consumer.accept(new RenderedFeature(
      tile,
      new VectorTile.Feature(
        feature.getLayer(),
        id,
        VectorTile.encodeGeometry(geom, scale),
        attrs,
        groupInfo == null ? VectorTile.Feature.NO_GROUP : groupInfo.group()
      ),
      feature.getSortKey(),
      Optional.ofNullable(groupInfo)
    ));
  }

  private void renderPoint(int zoom, Map<String, Object> attrs, FeatureCollector.Feature feature, MultiPoint points) {
    /*
     * Attempt to encode multipoints as a single feature sharing attributes and sort-key
     * but if it has label grid data then need to fall back to separate features per point,
     * so they can be filtered individually.
     */
    if (feature.hasLabelGrid()) {
      for (Coordinate coord : points.getCoordinates()) {
        renderPoint(zoom, attrs, feature, coord);
      }
    } else {
      renderPoint(zoom, attrs, feature, points.getCoordinates());
    }
  }

  private void renderLineOrPolygon(int zoom, Map<String, Object> attrs, FeatureCollector.Feature feature,
    Geometry geom) {
    boolean finished = false;
    boolean area = geom instanceof Polygonal;
    double minSize = feature.getMinPixelSizeAtZoom(zoom) / 256d;
    double buffer = feature.getBufferPixelsAtZoom(zoom) / 256;
    if (area) {
      // treat minPixelSize as the edge of a square that defines minimum area for features
      minSize *= minSize;
    }
    TileExtents.ForZoom extents = config.bounds().tileExtents().getForZoom(zoom);
    TiledGeometry sliced = null;
    List<List<CoordinateSequence>> groups = GeometryCoordinateSequences.extractGroups(geom, minSize);
    try {
      sliced = TiledGeometry.sliceIntoTiles(groups, buffer, area, zoom, extents);
    } catch (GeometryException e) {
      try {
        geom = GeoUtils.fixPolygon(geom);
        groups = GeometryCoordinateSequences.extractGroups(geom, minSize);
        sliced = TiledGeometry.sliceIntoTiles(groups, buffer, area, zoom, extents);
      } catch (GeometryException ex) {
        ex.log(stats, "slice_line_or_polygon", "Error slicing feature at z" + zoom + ": " + feature);
        // omit from this zoom level, but maybe the next will be better
        finished = true;
      }
    }
    if (!finished) {
      String numPointsAttr = feature.getNumPointsAttr();
      if (numPointsAttr != null) {
        // if profile wants the original number off points that the simplified but untiled geometry started with
        attrs = new HashMap<>(attrs);
        attrs.put(numPointsAttr, geom.getNumPoints());
      }
      writeTileFeatures(zoom, feature.getId(), feature, sliced, attrs);
    }

    stats.processedElement(area ? "polygon" : "line", feature.getLayer(), zoom);
  }

  private void writeTileFeatures(int zoom, long id, FeatureCollector.Feature feature, TiledGeometry sliced,
    Map<String, Object> attrs) {
    int emitted = 0;
    for (var entry : sliced.getTileData().entrySet()) {
      TileCoord tile = entry.getKey();
      try {
        List<List<CoordinateSequence>> geoms = entry.getValue();

        Geometry geom;
        int scale = 0;
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
          geom = GeoUtils.snapAndFixPolygon(geom, stats, "render");
          // JTS utilities "fix" the geometry to be clockwise outer/CCW inner but vector tiles flip Y coordinate,
          // so we need outer CCW/inner clockwise
          geom = geom.reverse();
        } else {
          geom = GeometryCoordinateSequences.reassembleLineStrings(geoms);
          // Store lines with extra precision (2^scale) in intermediate feature storage so that
          // rounding does not introduce artificial endpoint intersections and confuse line merge
          // post-processing.  Features need to be "unscaled" in FeatureGroup after line merging,
          // and before emitting to the output archive.
          scale = Math.max(config.maxzoom(), 14) - zoom;
          // need 14 bits to represent tile coordinates (4096 * 2 for buffer * 2 for zigzag encoding)
          // so cap the scale factor to avoid overflowing 32-bit integer space
          scale = Math.min(31 - 14, scale);
        }

        if (!geom.isEmpty()) {
          encodeAndEmitFeature(feature, id, attrs, tile, geom, null, scale);
          emitted++;
        }
      } catch (GeometryException e) {
        e.log(stats, "write_tile_features", "Error writing tile " + tile + " feature " + feature);
      }
    }

    // polygons that span multiple tiles contain detail about the outer edges separate from the filled tiles, so emit
    // filled tiles now
    if (feature.isPolygon()) {
      emitted += emitFilledTiles(zoom, id, feature, sliced);
    }

    stats.emittedFeatures(zoom, feature.getLayer(), emitted);
  }

  private int emitFilledTiles(int zoom, long id, FeatureCollector.Feature feature, TiledGeometry sliced) {
    Optional<RenderedFeature.Group> groupInfo = Optional.empty();
    /*
     * Optimization: large input polygons that generate many filled interior tiles (i.e. the ocean), the encoder avoids
     * re-encoding if groupInfo and vector tile feature are == to previous values, so compute one instance at the start
     * of each zoom level for this feature.
     */
    VectorTile.Feature vectorTileFeature = new VectorTile.Feature(
      feature.getLayer(),
      id,
      VectorTile.encodeFill(feature.getBufferPixelsAtZoom(zoom)),
      feature.getAttrsAtZoom(sliced.zoomLevel())
    );

    int emitted = 0;
    for (TileCoord tile : sliced.getFilledTiles()) {
      consumer.accept(new RenderedFeature(
        tile,
        vectorTileFeature,
        feature.getSortKey(),
        groupInfo
      ));
      emitted++;
    }
    return emitted;
  }

  @Override
  public void close() {
    if (closeable != null) {
      try {
        closeable.close();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }
}
