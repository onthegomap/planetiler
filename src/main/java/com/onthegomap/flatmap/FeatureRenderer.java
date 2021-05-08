package com.onthegomap.flatmap;

import com.onthegomap.flatmap.collections.FeatureSort;
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

  public FeatureRenderer(CommonParams config) {
    this.config = config;
  }

  public void renderFeature(FeatureCollector.Feature feature, Consumer<FeatureSort.Entry> consumer) {
    renderGeometry(feature.geometry(), feature, consumer);
  }

  public void renderGeometry(Geometry geom, FeatureCollector.Feature feature, Consumer<FeatureSort.Entry> consumer) {
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

  private void addPointFeature(FeatureCollector.Feature feature, Point point, Consumer<FeatureSort.Entry> consumer) {
    // TODO render features into tile
  }

  private void addPointFeature(FeatureCollector.Feature feature, MultiPoint points,
    Consumer<FeatureSort.Entry> consumer) {
    // TODO render features into tile
  }

  private void addLinearFeature(FeatureCollector.Feature feature, Geometry geom,
    Consumer<FeatureSort.Entry> consumer) {
    // TODO render lines / areas into tile
  }
}
