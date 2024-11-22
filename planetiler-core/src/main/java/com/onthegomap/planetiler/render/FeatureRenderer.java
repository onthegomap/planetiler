package com.onthegomap.planetiler.render;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.VectorTile;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.DouglasPeuckerSimplifier;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.GeometryException;
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
public class FeatureRenderer implements Consumer<FeatureCollector.Feature>, Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureRenderer.class);
  private static final VectorTile.VectorGeometry FILL = VectorTile.encodeGeometry(GeoUtils.JTS_FACTORY
    .createPolygon(GeoUtils.JTS_FACTORY.createLinearRing(new PackedCoordinateSequence.Double(new double[]{
      -5, -5,
      261, -5,
      261, 261,
      -5, 261,
      -5, -5
    }, 2, 0))));
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
    renderGeometry(feature.getGeometry(), feature);
  }

  private void renderGeometry(Geometry geom, FeatureCollector.Feature feature) {
    if (geom.isEmpty()) {
      LOGGER.warn("Empty geometry {}", feature);
    } else if (geom instanceof Point point) {
      renderPoint(feature, point.getCoordinates());
    } else if (geom instanceof MultiPoint points) {
      renderPoint(feature, points);
    } else if (geom instanceof Polygon || geom instanceof MultiPolygon || geom instanceof LineString ||
      geom instanceof MultiLineString) {
      renderLineOrPolygon(feature, geom);
    } else if (geom instanceof GeometryCollection collection) {
      for (int i = 0; i < collection.getNumGeometries(); i++) {
        renderGeometry(collection.getGeometryN(i), feature);
      }
    } else {
      LOGGER.warn("Unrecognized JTS geometry type for {}: {}", feature.getClass().getSimpleName(),
        geom.getGeometryType());
    }
  }

  private void renderPoint(FeatureCollector.Feature feature, Coordinate... origCoords) {
    boolean hasLabelGrid = feature.hasLabelGrid();
    Coordinate[] coords = new Coordinate[origCoords.length];
    for (int i = 0; i < origCoords.length; i++) {
      coords[i] = origCoords[i].copy();
    }
    for (int zoom = feature.getMaxZoom(); zoom >= feature.getMinZoom(); zoom--) {
      double minSize = feature.getMinPixelSizeAtZoom(zoom);
      if (minSize > 0 && feature.getSourceFeaturePixelSizeAtZoom(zoom) < minSize) {
        continue;
      }
      Map<String, Object> attrs = feature.getAttrsAtZoom(zoom);
      double buffer = feature.getBufferPixelsAtZoom(zoom) / 256;
      int tilesAtZoom = 1 << zoom;
      // scale coordinates for this zoom
      for (int i = 0; i < coords.length; i++) {
        var orig = origCoords[i];
        coords[i].setX(orig.x * tilesAtZoom);
        coords[i].setY(orig.y * tilesAtZoom);
      }


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
    }

    stats.processedElement("point", feature.getLayer());
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

  private void renderPoint(FeatureCollector.Feature feature, MultiPoint points) {
    /*
     * Attempt to encode multipoints as a single feature sharing attributes and sort-key
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
    boolean area = input instanceof Polygonal;
    double worldLength = (area || input.getNumGeometries() > 1) ? 0 : input.getLength();
    for (int z = feature.getMaxZoom(); z >= feature.getMinZoom(); z--) {
      double scale = 1 << z;
      double minSize = feature.getMinPixelSizeAtZoom(z) / 256d;
      if (area) {
        // treat minPixelSize as the edge of a square that defines minimum area for features
        minSize *= minSize;
      } else if (worldLength > 0 && worldLength * scale < minSize) {
        // skip linestring, too short
        continue;
      }

      if (feature.hasLinearRanges()) {
        for (var range : feature.getLinearRangesAtZoom(z)) {
          if (worldLength * scale * (range.end() - range.start()) >= minSize) {
            renderLineOrPolygonGeometry(feature, range.geom(), range.attrs(), z, minSize, area);
          }
        }
      } else {
        renderLineOrPolygonGeometry(feature, input, feature.getAttrsAtZoom(z), z, minSize, area);
      }
    }

    stats.processedElement(area ? "polygon" : "line", feature.getLayer());
  }

  private void renderLineOrPolygonGeometry(FeatureCollector.Feature feature, Geometry input, Map<String, Object> attrs,
    int z, double minSize, boolean area) {
    double scale = 1 << z;
    double tolerance = feature.getPixelToleranceAtZoom(z) / 256d;
    SimplifyMethod simplifyMethod = feature.getSimplifyMethodAtZoom(z);
    double buffer = feature.getBufferPixelsAtZoom(z) / 256;
    TileExtents.ForZoom extents = config.bounds().tileExtents().getForZoom(z);

    // TODO potential optimization: iteratively simplify z+1 to get z instead of starting with original geom each time
    // simplify only takes 4-5 minutes of wall time when generating the planet though, so not a big deal
    Geometry scaled = AffineTransformation.scaleInstance(scale, scale).transform(input);
    TiledGeometry sliced;
    // TODO replace with geometry pipeline when available
    Geometry geom = switch (simplifyMethod) {
      case RETAIN_IMPORTANT_POINTS -> DouglasPeuckerSimplifier.simplify(scaled, tolerance);
      // DP tolerance is displacement, and VW tolerance is area, so square what the user entered to convert from
      // DP to VW tolerance
      case RETAIN_EFFECTIVE_AREAS -> new VWSimplifier().setTolerance(tolerance * tolerance).transform(scaled);
      case RETAIN_WEIGHTED_EFFECTIVE_AREAS ->
        new VWSimplifier().setWeight(0.7).setTolerance(tolerance * tolerance).transform(scaled);
    };
    List<List<CoordinateSequence>> groups = GeometryCoordinateSequences.extractGroups(geom, minSize);
    try {
      sliced = TiledGeometry.sliceIntoTiles(groups, buffer, area, z, extents);
    } catch (GeometryException e) {
      try {
        geom = GeoUtils.fixPolygon(geom);
        groups = GeometryCoordinateSequences.extractGroups(geom, minSize);
        sliced = TiledGeometry.sliceIntoTiles(groups, buffer, area, z, extents);
      } catch (GeometryException ex) {
        ex.log(stats, "slice_line_or_polygon", "Error slicing feature at z" + z + ": " + feature);
        // omit from this zoom level, but maybe the next will be better
        return;
      }
    }
    String numPointsAttr = feature.getNumPointsAttr();
    if (numPointsAttr != null) {
      // if profile wants the original number off points that the simplified but untiled geometry started with
      attrs = new HashMap<>(attrs);
      attrs.put(numPointsAttr, geom.getNumPoints());
    }
    writeTileFeatures(z, feature.getId(), feature, sliced, attrs);
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
