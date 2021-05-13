package com.onthegomap.flatmap;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Lineal;
import org.locationtech.jts.geom.Puntal;

public class FeatureCollector implements Iterable<FeatureCollector.Feature<?>> {

  private static final AtomicLong idGen = new AtomicLong(0);
  private final SourceFeature source;
  private final List<Feature<?>> output = new ArrayList<>();
  private final CommonParams config;

  private FeatureCollector(SourceFeature source, CommonParams config) {
    this.source = source;
    this.config = config;
  }


  public static record Factory(CommonParams config) {

    public FeatureCollector get(SourceFeature source) {
      return new FeatureCollector(source, config);
    }
  }

  @Override
  public Iterator<Feature<?>> iterator() {
    return output.iterator();
  }

  public PointFeature point(String layer) {
    var feature = new PointFeature(layer, source.centroid());
    output.add(feature);
    return feature;
  }

  public LineFeature line(String layername) {
    var feature = new LineFeature(layername, source.line());
    output.add(feature);
    return feature;
  }

  public PolygonFeature polygon(String layername) {
    var feature = new PolygonFeature(layername, source.polygon());
    output.add(feature);
    return feature;
  }

  public class PointFeature extends Feature<PointFeature> {

    private static final double DEFAULT_LABEL_GRID_SIZE = 0;
    private static final int DEFAULT_LABEL_GRID_LIMIT = 0;

    private ZoomFunction<Number> labelGridPixelSize = null;
    private ZoomFunction<Number> labelGridLimit = null;

    private PointFeature(String layer, Geometry geom) {
      super(layer, geom);
      assert geom instanceof Puntal;
    }

    public double getLabelGridPixelSizeAtZoom(int zoom) {
      return ZoomFunction.applyAsDoubleOrElse(labelGridPixelSize, zoom, DEFAULT_LABEL_GRID_SIZE);
    }

    public int getLabelGridLimitAtZoom(int zoom) {
      return ZoomFunction.applyAsIntOrElse(labelGridLimit, zoom, DEFAULT_LABEL_GRID_LIMIT);
    }

    private PointFeature setLabelGridPixelSizeFunction(ZoomFunction<Number> labelGridSize) {
      this.labelGridPixelSize = labelGridSize;
      return this;
    }

    private PointFeature setLabelGridLimitFunction(ZoomFunction<Number> labelGridLimit) {
      this.labelGridLimit = labelGridLimit;
      return this;
    }

    public PointFeature setLabelGridPixelSize(int maxzoom, double size) {
      return setLabelGridPixelSizeFunction(ZoomFunction.maxZoom(maxzoom, size));
    }

    public PointFeature setLabelGridSizeAndLimit(int maxzoom, double size, int limit) {
      return setLabelGridPixelSizeFunction(ZoomFunction.maxZoom(maxzoom, size))
        .setLabelGridLimitFunction(ZoomFunction.maxZoom(maxzoom, limit));
    }

    @Override
    protected PointFeature self() {
      return this;
    }
  }

  public class LineFeature extends Feature<LineFeature> {

    private double defaultMinLength = 1;
    private ZoomFunction<Number> minLength = null;

    private LineFeature(String layer, Geometry geom) {
      super(layer, geom);
      assert geom instanceof Lineal;
    }

    public double getMinLengthAtZoom(int zoom) {
      return ZoomFunction.applyAsDoubleOrElse(minLength, zoom, defaultMinLength);
    }

    public LineFeature setMinLength(double minLength) {
      this.defaultMinLength = minLength;
      return this;
    }

    public LineFeature setMinLengthBelowZoom(int zoom, double minLength) {
      this.minLength = ZoomFunction.maxZoom(zoom, minLength);
      return this;
    }

    @Override
    protected LineFeature self() {
      return this;
    }
  }

  public class PolygonFeature extends Feature<PolygonFeature> {

    private double defaultMinArea = 1;
    private ZoomFunction<Number> minArea = null;

    private PolygonFeature(String layer, Geometry geom) {
      super(layer, geom);
    }

    public double getMinAreaAtZoom(int zoom) {
      return ZoomFunction.applyAsDoubleOrElse(minArea, zoom, defaultMinArea);
    }

    public PolygonFeature setMinArea(double minArea) {
      this.defaultMinArea = minArea;
      return this;
    }

    public PolygonFeature setMinAreaBelowZoom(int zoom, double minArea) {
      this.minArea = ZoomFunction.maxZoom(zoom, minArea);
      return this;
    }

    @Override
    protected PolygonFeature self() {
      return this;
    }
  }

  public abstract class Feature<T extends Feature<T>> {

    private final String layer;
    private final Geometry geom;
    private final long id;
    private int zOrder;
    private int minzoom = config.minzoom();
    private int maxzoom = config.maxzoom();
    private double defaultBuffer;
    private ZoomFunction<Number> bufferOverrides;
    private final Map<String, Object> attrs = new TreeMap<>();

    private Feature(String layer, Geometry geom) {
      this.layer = layer;
      this.geom = geom;
      this.id = idGen.incrementAndGet();
      this.zOrder = 0;
    }

    protected abstract T self();

    public T setZorder(int zOrder) {
      this.zOrder = zOrder;
      return self();
    }

    public int getZorder() {
      return zOrder;
    }

    public T setZoomRange(int min, int max) {
      return setMinZoom(min).setMaxZoom(max);
    }

    public T setMinZoom(int min) {
      minzoom = Math.max(min, config.minzoom());
      return self();
    }

    public int getMinZoom() {
      return minzoom;
    }

    public T setMaxZoom(int max) {
      maxzoom = Math.min(max, config.maxzoom());
      return self();
    }

    public int getMaxZoom() {
      return maxzoom;
    }

    public long getId() {
      return id;
    }

    public String getLayer() {
      return layer;
    }

    public Geometry getGeometry() {
      return geom;
    }

    public double getBufferAtZoom(int zoom) {
      return ZoomFunction.applyAsDoubleOrElse(bufferOverrides, zoom, defaultBuffer);
    }

    public T setBuffer(double buffer) {
      defaultBuffer = buffer;
      return self();
    }

    public T setBufferOverrides(ZoomFunction<Number> buffer) {
      bufferOverrides = buffer;
      return self();
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

    public T inheritFromSource(String attr) {
      return setAttr(attr, source.getTag(attr));
    }

    public T setAttr(String key, Object value) {
      attrs.put(key, value);
      return self();
    }

    public T setAttrWithMinzoom(String key, Object value, int minzoom) {
      attrs.put(key, ZoomFunction.minZoom(minzoom, value));
      return self();
    }

  }
}
