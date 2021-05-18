package com.onthegomap.flatmap;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.locationtech.jts.geom.Geometry;

public class FeatureCollector implements Iterable<FeatureCollector.Feature> {

  private final SourceFeature source;
  private final List<Feature> output = new ArrayList<>();
  private final CommonParams config;

  private FeatureCollector(SourceFeature source, CommonParams config) {
    this.source = source;
    this.config = config;
  }

  @Override
  public Iterator<Feature> iterator() {
    return output.iterator();
  }

  public Feature point(String layer) {
    var feature = new Feature(layer, source.isPoint() ? source.worldGeometry() : source.centroid());
    output.add(feature);
    return feature;
  }

  public Feature line(String layername) {
    var feature = new Feature(layername, source.line());
    output.add(feature);
    return feature;
  }

  public Feature polygon(String layername) {
    var feature = new Feature(layername, source.polygon());
    output.add(feature);
    return feature;
  }

  public static record Factory(CommonParams config) {

    public FeatureCollector get(SourceFeature source) {
      return new FeatureCollector(source, config);
    }
  }

  public final class Feature {

    private static final double DEFAULT_LABEL_GRID_SIZE = 0;
    private static final int DEFAULT_LABEL_GRID_LIMIT = 0;
    private final String layer;
    private final Geometry geom;
    private final Map<String, Object> attrs = new TreeMap<>();
    private int zOrder;
    private int minzoom = config.minzoom();
    private int maxzoom = config.maxzoom();
    private double defaultBufferPixels = 4;
    private ZoomFunction<Number> bufferPixelOverrides;
    private double defaultMinPixelSize = 1;
    private ZoomFunction<Number> minPixelSize = null;
    private ZoomFunction<Number> labelGridPixelSize = null;
    private ZoomFunction<Number> labelGridLimit = null;

    private Feature(String layer, Geometry geom) {
      this.layer = layer;
      this.geom = geom;
      this.zOrder = 0;
    }

    public int getZorder() {
      return zOrder;
    }

    public Feature setZorder(int zOrder) {
      this.zOrder = zOrder;
      return this;
    }

    public Feature setZoomRange(int min, int max) {
      return setMinZoom(min).setMaxZoom(max);
    }

    public int getMinZoom() {
      return minzoom;
    }

    public Feature setMinZoom(int min) {
      minzoom = Math.max(min, config.minzoom());
      return this;
    }

    public int getMaxZoom() {
      return maxzoom;
    }

    public Feature setMaxZoom(int max) {
      maxzoom = Math.min(max, config.maxzoom());
      return this;
    }

    public String getLayer() {
      return layer;
    }

    public Geometry getGeometry() {
      return geom;
    }

    public double getBufferPixelsAtZoom(int zoom) {
      return ZoomFunction.applyAsDoubleOrElse(bufferPixelOverrides, zoom, defaultBufferPixels);
    }

    public Feature setBufferPixels(double buffer) {
      defaultBufferPixels = buffer;
      return this;
    }

    public Feature setBufferPixelOverrides(ZoomFunction<Number> buffer) {
      bufferPixelOverrides = buffer;
      return this;
    }

    public double getMinPixelSize(int zoom) {
      return ZoomFunction.applyAsDoubleOrElse(minPixelSize, zoom, defaultMinPixelSize);
    }

    public Feature setMinPixelSize(double minPixelSize) {
      this.defaultMinPixelSize = minPixelSize;
      return this;
    }

    public Feature setMinPixelSizeBelowZoom(int zoom, double minPixelSize) {
      this.minPixelSize = ZoomFunction.maxZoom(zoom, minPixelSize);
      return this;
    }

    public double getLabelGridPixelSizeAtZoom(int zoom) {
      return ZoomFunction.applyAsDoubleOrElse(labelGridPixelSize, zoom, DEFAULT_LABEL_GRID_SIZE);
    }

    public int getLabelGridLimitAtZoom(int zoom) {
      return ZoomFunction.applyAsIntOrElse(labelGridLimit, zoom, DEFAULT_LABEL_GRID_LIMIT);
    }

    private Feature setLabelGridPixelSizeFunction(ZoomFunction<Number> labelGridSize) {
      this.labelGridPixelSize = labelGridSize;
      return this;
    }

    private Feature setLabelGridLimitFunction(ZoomFunction<Number> labelGridLimit) {
      this.labelGridLimit = labelGridLimit;
      return this;
    }

    public Feature setLabelGridPixelSize(int maxzoom, double size) {
      return setLabelGridPixelSizeFunction(ZoomFunction.maxZoom(maxzoom, size));
    }

    public Feature setLabelGridSizeAndLimit(int maxzoom, double size, int limit) {
      return setLabelGridPixelSizeFunction(ZoomFunction.maxZoom(maxzoom, size))
        .setLabelGridLimitFunction(ZoomFunction.maxZoom(maxzoom, limit));
    }

    public boolean hasLabelGrid() {
      return labelGridPixelSize != null || labelGridLimit != null;
    }

    public Map<String, Object> getAttrsAtZoom(int zoom) {
      Map<String, Object> result = new TreeMap<>();
      for (var entry : attrs.entrySet()) {
        Object value = entry.getValue();
        if (value instanceof ZoomFunction<?> fn) {
          value = fn.apply(zoom);
        }
        if (value != null && !"".equals(value)) {
          result.put(entry.getKey(), value);
        }
      }
      return result;
    }

    public Feature inheritFromSource(String attr) {
      return setAttr(attr, source.getTag(attr));
    }

    public Feature setAttr(String key, Object value) {
      attrs.put(key, value);
      return this;
    }

    public Feature setAttrWithMinzoom(String key, Object value, int minzoom) {
      attrs.put(key, ZoomFunction.minZoom(minzoom, value));
      return this;
    }

    @Override
    public String toString() {
      return "Feature{" +
        "layer='" + layer + '\'' +
        ", geom=" + geom.getGeometryType() +
        ", attrs=" + attrs +
        '}';
    }
  }
}
