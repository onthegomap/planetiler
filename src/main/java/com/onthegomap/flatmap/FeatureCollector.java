package com.onthegomap.flatmap;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.locationtech.jts.geom.Geometry;

public class FeatureCollector implements Iterable<FeatureCollector.Feature<?>> {

  private static final AtomicLong idGen = new AtomicLong(0);
  private final SourceFeature source;
  private final List<Feature<?>> output = new ArrayList<>();
  private final CommonParams config;

  private FeatureCollector(SourceFeature source, CommonParams config) {
    this.source = source;
    this.config = config;
  }

  public static FeatureCollector from(SourceFeature source, CommonParams config) {
    return new FeatureCollector(source, config);
  }

  @Override
  public Iterator<Feature<?>> iterator() {
    return output.iterator();
  }

  public PointFeature point(String layer) {
    return new PointFeature(layer, source.geometry().getCentroid());
  }

  public class PointFeature extends Feature<PointFeature> {

    private static final double DEFAULT_LABEL_GRID_SIZE = 0;
    private static final int DEFAULT_LABEL_GRID_LIMIT = 0;

    private ZoomDependent.DoubleValue labelGridSize = null;
    private ZoomDependent.IntValue labelGridLimit = null;

    private PointFeature(String layer, Geometry geom) {
      super(layer, geom);
    }

    public double getLabelGridSizeAtZoom(int zoom) {
      return labelGridSize == null ? DEFAULT_LABEL_GRID_SIZE : labelGridSize.getDoubleValueAtZoom(zoom);
    }

    public int getLabelGridLimitAtZoom(int zoom) {
      return labelGridLimit == null ? DEFAULT_LABEL_GRID_LIMIT : labelGridLimit.getIntValueAtZoom(zoom);
    }

    @Override
    protected PointFeature self() {
      return this;
    }
  }

  public class LineFeature extends Feature<LineFeature> {

    private double defaultMinLength = 1;
    private ZoomDependent.DoubleValue minLength = null;

    private LineFeature(String layer, Geometry geom) {
      super(layer, geom);
    }

    public double getMinLengthAtZoom(int zoom) {
      return minLength == null ? defaultMinLength : minLength.getDoubleValueAtZoom(zoom);
    }

    @Override
    protected LineFeature self() {
      return this;
    }
  }

  public class PolygonFeature extends Feature<PolygonFeature> {

    private double defaultMinArea = 1;
    private ZoomDependent.DoubleValue minArea = null;

    private PolygonFeature(String layer, Geometry geom) {
      super(layer, geom);
    }

    public double getMinAreaAtZoom(int zoom) {
      return minArea == null ? defaultMinArea : minArea.getDoubleValueAtZoom(zoom);
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
    private ZoomDependent.DoubleValue bufferOverrides;

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
      minzoom = min;
      return self();
    }

    public int getMinZoom() {
      return minzoom;
    }

    public T setMaxZoom(int max) {
      maxzoom = max;
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
      return bufferOverrides == null ? defaultBuffer : bufferOverrides.getDoubleValueAtZoom(zoom);
    }

    public Map<String, Object> getAttrsAtZoom(int zoom) {
      TODO want minzoom / maxzoom per attr
      different values per attr by zoom
    }

    public T inheritFromSource(String attr) {
      return setAttr(attr, source.getTag(attr));
    }

    public T setAttr(String key, Object value) {
      return self();
    }
  }
}
