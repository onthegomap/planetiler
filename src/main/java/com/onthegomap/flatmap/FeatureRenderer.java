package com.onthegomap.flatmap;

import com.onthegomap.flatmap.geo.GeoUtils;
import com.onthegomap.flatmap.geo.TileCoord;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.jetbrains.annotations.NotNull;
import org.locationtech.jts.algorithm.Area;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.CoordinateSequences;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
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

  private static double wrapDouble(double value, double max) {
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
        // TODO TiledGeometry.sliceIntoTiles(...)
        slicePoint(sliced, zoom, buffer, coord);
      }

      RenderedFeature.Group groupInfo = null;
      if (feature.hasLabelGrid() && coords.length == 1) {
        double labelGridTileSize = feature.getLabelGridPixelSizeAtZoom(zoom) / 256d;
        groupInfo = labelGridTileSize >= 1d / 4096d ? new RenderedFeature.Group(GeoUtils.longPair(
          (int) Math.floor(wrapDouble(coords[0].getX() * tilesAtZoom, tilesAtZoom) / labelGridTileSize),
          (int) Math.floor((coords[0].getY() * tilesAtZoom) / labelGridTileSize)
        ), feature.getLabelGridLimitAtZoom(zoom)) : null;
      }

      for (var entry : sliced.entrySet()) {
        TileCoord tile = entry.getKey();
        Set<Coordinate> value = entry.getValue();
        Geometry geom = value.size() == 1 ? GeoUtils.point(value.iterator().next()) : GeoUtils.multiPoint(value);
        // TODO stats
        // TODO writeTileFeatures
        emitFeature(feature, id, attrs, groupInfo, tile, geom);
      }
    }
  }

  private void emitFeature(FeatureCollector.Feature feature, long id, TileCoord tile, Geometry geom) {
    emitFeature(feature, id, feature.getAttrsAtZoom(tile.z()), null, tile, geom);
  }

  private void emitFeature(FeatureCollector.Feature feature, long id, Map<String, Object> attrs,
    RenderedFeature.Group groupInfo, TileCoord tile, Geometry geom) {
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

      List<List<CoordinateSequence>> groups = new ArrayList<>();
      extractGroups(geom, groups, minSize);
      double buffer = feature.getBufferPixelsAtZoom(z);
      TileExtents.ForZoom extents = config.extents().getForZoom(z);
      TiledGeometry sliced = TiledGeometry.sliceIntoTiles(groups, buffer, area, z, extents);
      writeTileFeatures(id, feature, sliced);
    }
  }

  private void writeTileFeatures(long id, FeatureCollector.Feature feature, TiledGeometry sliced) {
    for (var entry : sliced.getTileData()) {
      TileCoord tile = entry.getKey();
      List<List<CoordinateSequence>> geoms = entry.getValue();

      Geometry geom;
      if (feature.area()) {
        geom = reassemblePolygon(feature, tile, geoms);
      } else {
        geom = reassembleLineString(geoms);
      }

      try {
        geom = GeometryPrecisionReducer.reduce(geom, tilePrecision);
      } catch (IllegalArgumentException e) {
        LOGGER.warn("Error reducing precision of " + feature + " on " + tile + ": " + e);
      }

      if (!geom.isEmpty()) {
        // JTS utilities "fix" the geometry to be clockwise outer/CCW inner
        if (feature.area()) {
          geom = geom.reverse();
        }
        emitFeature(feature, id, tile, geom);
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
     * re-encoding if groupInfo and vector tile feature are == to previous values. The feature can have different
     * attributes at different zoom levels though, so need to cache each vector tile feature instance by zoom level.
     */
    Optional<RenderedFeature.Group> groupInfo = Optional.empty();
    VectorTileEncoder.Feature cachedFeature = null;
    int lastZoom = Integer.MIN_VALUE;

    for (TileCoord tile : sliced.getFilledTilesOrderedByZXY()) {
      int zoom = tile.z();
      if (zoom != lastZoom) {
        cachedFeature = new VectorTileEncoder.Feature(feature.getLayer(), id, FILL, feature.getAttrsAtZoom(zoom));
        lastZoom = zoom;
      }
      consumer.accept(new RenderedFeature(
        tile,
        cachedFeature,
        feature.getZorder(),
        groupInfo
      ));
    }
  }

  private Geometry reassembleLineString(List<List<CoordinateSequence>> geoms) {
    Geometry geom;
    List<LineString> lineStrings = new ArrayList<>();
    for (List<CoordinateSequence> inner : geoms) {
      for (CoordinateSequence coordinateSequence : inner) {
        lineStrings.add(GeoUtils.JTS_FACTORY.createLineString(coordinateSequence));
      }
    }
    geom = GeoUtils.createMultiLineString(lineStrings);
    return geom;
  }

  @NotNull
  private Geometry reassemblePolygon(FeatureCollector.Feature feature, TileCoord tile,
    List<List<CoordinateSequence>> geoms) {
    Geometry geom;
    int numGeoms = geoms.size();
    Polygon[] polygons = new Polygon[numGeoms];
    for (int i = 0; i < numGeoms; i++) {
      List<CoordinateSequence> group = geoms.get(i);
      LinearRing first = GeoUtils.JTS_FACTORY.createLinearRing(group.get(0));
      LinearRing[] rest = new LinearRing[group.size() - 1];
      for (int j = 1; j < group.size(); j++) {
        CoordinateSequence seq = group.get(j);
        CoordinateSequences.reverse(seq);
        rest[j - 1] = GeoUtils.JTS_FACTORY.createLinearRing(seq);
      }
      polygons[i] = GeoUtils.JTS_FACTORY.createPolygon(first, rest);
    }
    geom = GeoUtils.JTS_FACTORY.createMultiPolygon(polygons);
    if (!geom.isValid()) {
      geom = geom.buffer(0);
      if (!geom.isValid()) {
        geom = geom.buffer(0);
        if (!geom.isValid()) {
          LOGGER.warn("Geometry still invalid after 2 buffers " + feature + " on " + tile);
        }
      }
    }
    return geom;
  }

  private void extractGroups(Geometry geom, List<List<CoordinateSequence>> groups, double minSize) {
    if (geom.isEmpty()) {
      // ignore
    } else if (geom instanceof GeometryCollection) {
      for (int i = 0; i < geom.getNumGeometries(); i++) {
        extractGroups(geom.getGeometryN(i), groups, minSize);
      }
    } else if (geom instanceof Polygon polygon) {
      extractGroupsFromPolygon(groups, minSize, polygon);
    } else if (geom instanceof LinearRing linearRing) {
      extractGroups(GeoUtils.JTS_FACTORY.createPolygon(linearRing), groups, minSize);
    } else if (geom instanceof LineString lineString) {
      if (lineString.getLength() >= minSize) {
        groups.add(List.of(lineString.getCoordinateSequence()));
      }
    } else {
      throw new RuntimeException("unrecognized geometry type: " + geom.getGeometryType());
    }
  }

  private void extractGroupsFromPolygon(List<List<CoordinateSequence>> groups, double minSize, Polygon polygon) {
    CoordinateSequence outer = polygon.getExteriorRing().getCoordinateSequence();
    double outerArea = Area.ofRingSigned(outer);
    if (outerArea > 0) {
      CoordinateSequences.reverse(outer);
    }
    if (Math.abs(outerArea) >= minSize) {
      List<CoordinateSequence> group = new ArrayList<>(1 + polygon.getNumInteriorRing());
      groups.add(group);
      group.add(outer);
      for (int i = 0; i < polygon.getNumInteriorRing(); i++) {
        CoordinateSequence inner = polygon.getInteriorRingN(i).getCoordinateSequence();
        double innerArea = Area.ofRingSigned(inner);
        if (innerArea > 0) {
          CoordinateSequences.reverse(inner);
        }
        if (Math.abs(innerArea) >= minSize) {
          group.add(inner);
        }
      }
    }
  }
}
