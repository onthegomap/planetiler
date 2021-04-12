package com.onthegomap.flatmap;

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
  private final FlatMapConfig config;

  public FeatureRenderer(FlatMapConfig config) {
    this.config = config;
  }

  public void renderFeature(RenderableFeature feature, Consumer<RenderedFeature> consumer) {
    renderGeometry(feature.getGeometry(), feature, consumer);
  }

  public void renderGeometry(Geometry geom, RenderableFeature feature, Consumer<RenderedFeature> consumer) {
    // TODO what about converting between area and line?
    if (geom instanceof Point point) {
      addPointFeature(feature, point, consumer);
    } else if (geom instanceof MultiPoint points) {
      addPointFeature(feature, points, consumer);
    } else if (geom instanceof Polygon || geom instanceof MultiPolygon || geom instanceof LineString
      || geom instanceof MultiLineString) {
      addLinearFeature(feature, geom, consumer);
    } else if (geom instanceof GeometryCollection collection) {
      for (int i = 0; i < collection.getNumGeometries(); i++) {
        renderGeometry(collection.getGeometryN(i), feature, consumer);
      }
    } else {
      LOGGER.warn("Unrecognized JTS geometry type:" + geom.getGeometryType());
    }
  }

  private void addPointFeature(RenderableFeature feature, Point point, Consumer<RenderedFeature> consumer) {
    TODO render features into tile
  }

  private void addPointFeature(RenderableFeature feature, MultiPoint points, Consumer<RenderedFeature> consumer) {
    TODO render features into tile
  }

  private void addLinearFeature(RenderableFeature feature, Geometry geom, Consumer<RenderedFeature> consumer) {
    TODO render lines / areas into tile
  }
}
