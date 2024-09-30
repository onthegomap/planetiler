package com.onthegomap.planetiler.render;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.VectorTile;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.DouglasPeuckerSimplifier;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.geo.TileExtents;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.util.ImprovedFeatureRasterization;
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
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
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
    double buffer = feature.getBufferPixelsAtZoom(z) / 256;
    TileExtents.ForZoom extents = config.bounds().tileExtents().getForZoom(z);

    // TODO potential optimization: iteratively simplify z+1 to get z instead of starting with original geom each time
    // simplify only takes 4-5 minutes of wall time when generating the planet though, so not a big deal
    Geometry scaled = AffineTransformation.scaleInstance(scale, scale).transform(input);
    TiledGeometry sliced;
    Geometry geom = DouglasPeuckerSimplifier.simplify(scaled, tolerance);
//    Geometry geom = TopologyPreservingSimplifier.simplify(scaled, tolerance);
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

    writeTileFeatures(z, feature.getId(), feature, sliced, attrs, geom.getCoordinate());
  }

  private void writeTileFeatures_labelGrid(int zoom, long id, FeatureCollector.Feature feature, TiledGeometry sliced,
    Map<String, Object> attrs) {
    int emitted = 0;
    RenderedFeature.Group groupInfo = null;
    for (var entry : sliced.getTileData().entrySet()) {
      TileCoord tile = entry.getKey();
      try {
        List<List<CoordinateSequence>> geoms = entry.getValue();
        Coordinate centroid = null;
        Geometry geom;
        int scale = 0;
        if (feature.isPolygon()) {
          geom = GeometryCoordinateSequences.reassemblePolygons(geoms);

          geom = GeoUtils.snapAndFixPolygon(geom, stats, "render");
          if (!geom.isEmpty()) {
            centroid = convertCoordToGlobal(tile, new CoordinateXY(geom.getCentroid().getX(), geom.getCentroid().getY()));
          }
          geom = geom.reverse();
        } else {
          geom = GeometryCoordinateSequences.reassembleLineStrings(geoms);
          if (!geom.isEmpty()) {
            centroid = convertCoordToGlobal(tile, new CoordinateXY(geom.getCentroid().getX(), geom.getCentroid().getY()));
          }

          scale = Math.max(config.maxzoom(), 14) - zoom;

          scale = Math.min(31 - 14, scale);
        }

        if (!geom.isEmpty()) {
          // for "label grid" point density limiting, compute the grid square that this point sits in
          // only valid if not a multipoint

          boolean hasLabelGrid = feature.hasLabelGrid();
          if (hasLabelGrid) {
            int tilesAtZoom = 1 << zoom;
            Coordinate coord = new Coordinate(GeoUtils.getWorldX(centroid.x) * tilesAtZoom,
              GeoUtils.getWorldY(centroid.y) * tilesAtZoom);
            double labelGridTileSize = feature.getPointLabelGridPixelSizeAtZoom(zoom) / 256d;
            groupInfo = labelGridTileSize < 1d / 4096d ? null : new RenderedFeature.Group(
              GeoUtils.labelGridId(tilesAtZoom, labelGridTileSize, coord),
              feature.getPointLabelGridLimitAtZoom(zoom)
            );
          }

          //!attrs.containsKey("simply") && !feature.getAttrsAtZoom(zoom).containsKey("simply")
          if(false && !attrs.containsKey("simply") && !feature.getAttrsAtZoom(zoom).containsKey("simply")) {
//            Geometry pixelateGeometry = pixelateGeometry(geom,  tile,0.2, 1);
            Geometry pixelateGeometry = ImprovedFeatureRasterization.rasterizeGeometry(geom, tile, 1, 0.5);
              FeatureCollector.Feature simplyFeature = feature.collector().geometryNotAddOutPut(feature.getLayer(), pixelateGeometry)
              .setZoomRange(zoom,zoom);
            attrs.forEach(simplyFeature::setAttr);
            // 记录已经像素化
            simplyFeature.setAttr("simply", true);
            renderLineOrPolygon(simplyFeature, simplyFeature.getGeometry());
          } else {
            encodeAndEmitFeature(feature, id, attrs, tile, geom, groupInfo, scale);
            emitted++;
          }
        }
      } catch (GeometryException e) {
        e.log(stats, "write_tile_features", "Error writing tile " + tile + " feature " + feature);
      }
    }

    // polygons that span multiple tiles contain detail about the outer edges separate from the filled tiles, so emit
    // filled tiles now
    if (feature.isPolygon()) {
      emitted += emitFilledTiles_labelGrid(id, feature, sliced, groupInfo);
    }

    stats.emittedFeatures(zoom, feature.getLayer(), emitted);
  }

  private int emitFilledTiles_labelGrid(long id, FeatureCollector.Feature feature, TiledGeometry sliced,
    RenderedFeature.Group groupInfo) {
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
        Optional.ofNullable(groupInfo)
      ));
      emitted++;
    }
    return emitted;
  }

  private Coordinate convertCoordToGlobal(TileCoord tileCoord, Coordinate coordinate) {
    double worldWidthAtZoom = Math.pow(2, tileCoord.z());
    double minX = tileCoord.x() / worldWidthAtZoom;
    double maxX = (tileCoord.x() + 1) / worldWidthAtZoom;
    double minY = (tileCoord.y() + 1) / worldWidthAtZoom;
    double maxY = tileCoord.y() / worldWidthAtZoom;

    double relativeX = coordinate.getX() / 256.0;
    double relativeY = coordinate.getY() / 256.0;

    double worldX = minX + relativeX * (maxX - minX);
    double worldY = maxY - relativeY * (maxY - minY);

    double lon = normalizeLongitude(GeoUtils.getWorldLon(worldX));
    double lat = GeoUtils.getWorldLat(worldY);

    return new Coordinate(lon, lat);
  }

  private static double normalizeLongitude(double lon) {
    while (lon > 180) {
      lon -= 360;
    }
    while (lon < -180) {
      lon += 360;
    }
    return lon;
  }


  private void writeTileFeatures(int zoom, long id, FeatureCollector.Feature feature, TiledGeometry sliced,
    Map<String, Object> attrs, Coordinate coordinate) {
    int emitted = 0;
    for (var entry : sliced.getTileData().entrySet()) {
      TileCoord tile = entry.getKey();
      try {
        List<List<CoordinateSequence>> geoms = entry.getValue();

        Geometry geom;
        int scale = 0;
        PrecisionModel precision = null;
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
          // todo linespace
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

        RenderedFeature.Group groupInfo = null;
        if (!geom.isEmpty()) {
          if (config.pixelationZoom() > zoom && feature.hasLabelGrid()) {
            double labelGridTileSize = feature.getPointLabelGridPixelSizeAtZoom(zoom) / 256d;
            groupInfo = labelGridTileSize < 1d / 4096d ? null : new RenderedFeature.Group(
              GeoUtils.labelGridId(1 << zoom, labelGridTileSize, coordinate),
              feature.getPointLabelGridLimitAtZoom(zoom)
            );
          }

          encodeAndEmitFeature(feature, id, attrs, tile, geom, groupInfo, scale);
          emitted++;
        } else if (PlanetilerConfig.SmallFeatureStrategy.CENTROID.getStrategy().equals(config.smallFeatStrategy())
          && precision != null) {
          // todo linespace  避免在低级别要素被简化后丢弃，生成特征要素的质心代替原有要素，后续可考虑将质心优化为能够替代原有要素的最小三角形或正方形
          boolean isMaxZoom = zoom == config.maxzoom();
          if (isMaxZoom && (zoom == PlanetilerConfig.defaults().maxzoom() || zoom == PlanetilerConfig.MAX_MAXZOOM)) {
            LOGGER.debug("要素id: {} 在最高层级 {} 被简化!", feature.getId(), zoom);
          }

          Geometry centroid = feature.getGeometry().getCentroid();
          // 此处需要考虑最后一层级太小的要素应该如何处理，是否应该将simplyPoint赋值给feature
          FeatureCollector.Feature simplyPoint = feature.collector().geometryNotAddOutPut("linespace_layer_point",
              centroid)
            .setPointLabelGridPixelSize(config.labelGridPixelSize())
            .setPointLabelGridLimit(config.labelGridLimit())
            .setBufferPixelOverrides(config.bufferPixelOverrides())
            .setZoomRange(zoom, zoom);
          attrs.forEach(simplyPoint::setAttr);
          simplyPoint.setAttr("smallFeatureStrategy", PlanetilerConfig.SmallFeatureStrategy.SQUARE.getStrategy());

          feature.setAttr("smallFeatureStrategy", PlanetilerConfig.SmallFeatureStrategy.SQUARE.getStrategy());
          feature.setAttr("smallFeatureZoom", zoom);
          renderPoint(simplyPoint, centroid.getCoordinates());
        } else if (PlanetilerConfig.SmallFeatureStrategy.SQUARE.getStrategy().equals(config.smallFeatStrategy())
          && precision != null) {
          if (!attrs.containsKey("smallFeatureStrategy") && !feature.getAttrsAtZoom(zoom)
            .containsKey("smallFeatureStrategy")) {
            // 生成能替换要素的最小的正方形
            boolean isMaxZoom = zoom == config.maxzoom();
            if (isMaxZoom && (zoom == PlanetilerConfig.defaults().maxzoom() || zoom == PlanetilerConfig.MAX_MAXZOOM)) {
              LOGGER.debug("要素id: {} 在最高层级 {} 被简化!", feature.getId(), zoom);
            }

            double pixelSizeAtZoom = 1.0;
            if (config.pixelationZoom() > zoom && feature.hasLabelGrid()) {
              pixelSizeAtZoom = feature.getPointLabelGridPixelSizeAtZoom(zoom);
              double labelGridTileSize = pixelSizeAtZoom / 256d;
              groupInfo = labelGridTileSize < 1d / 4096d ? null : new RenderedFeature.Group(
                GeoUtils.labelGridId(1 << zoom, labelGridTileSize, coordinate),
                feature.getPointLabelGridLimitAtZoom(zoom)
              );
            }

            // 将坐标向下取整到最近的像素边界
            double x = Math.floor(coordinate.x / pixelSizeAtZoom) * pixelSizeAtZoom;
            double y = Math.floor(coordinate.y / pixelSizeAtZoom) * pixelSizeAtZoom;

            // 计算像素的四个角点坐标
            Coordinate[] coordinates = new Coordinate[5];
            coordinates[0] = new Coordinate(x, y);
            coordinates[1] = new Coordinate(x + pixelSizeAtZoom, y);
            coordinates[2] = new Coordinate(x + pixelSizeAtZoom, y + pixelSizeAtZoom);
            coordinates[3] = new Coordinate(x, y + pixelSizeAtZoom);
            coordinates[4] = new Coordinate(x, y);
            GeometryFactory geometryFactory = new GeometryFactory();
            geom =  geometryFactory.createPolygon(coordinates);
            encodeAndEmitFeature(feature, id, attrs, tile, geom, groupInfo, scale);

//            double precisionScale = precision.getScale();
//            Geometry centroid = feature.getGeometry().getCentroid();
//            // 生成矩形
//            Geometry squareGeom = GeoUtils.createSmallSquareWithCentroid(coordinate, precisionScale, zoom);
//            FeatureCollector.Feature simplySquare = feature.collector()
//              .geometryNotAddOutPut(feature.getLayer(), squareGeom)
//              .setMinPixelSizeAtAllZooms(0)
//              .setPixelToleranceAtAllZooms(0)
//              .setZoomRange(zoom, zoom);
//            attrs.forEach(simplySquare::setAttr);
//            simplySquare.setAttr("smallFeatureStrategy", PlanetilerConfig.SmallFeatureStrategy.SQUARE.getStrategy());
//
//            feature.setAttr("smallFeatureStrategy", PlanetilerConfig.SmallFeatureStrategy.SQUARE.getStrategy());
//            feature.setAttr("smallFeatureZoom", zoom);
//            renderLineOrPolygon(simplySquare, simplySquare.getGeometry());
          }
        } else {
          LOGGER.debug("Feature {} was simplified to empty geometry at zoom {}", feature, zoom);
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
