package com.onthegomap.flatmap;

import com.onthegomap.flatmap.collections.CacheByZoom;
import com.onthegomap.flatmap.geo.GeoUtils;
import com.onthegomap.flatmap.geo.GeometryException;
import com.onthegomap.flatmap.monitoring.Stats;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.locationtech.jts.geom.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FeatureCollector implements Iterable<FeatureCollector.Feature> {

  private static final Geometry EMPTY_GEOM = GeoUtils.JTS_FACTORY.createGeometryCollection();
  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureCollector.class);

  private final SourceFeature source;
  private final List<Feature> output = new ArrayList<>();
  private final CommonParams config;
  private final Stats stats;

  private FeatureCollector(SourceFeature source, CommonParams config, Stats stats) {
    this.source = source;
    this.config = config;
    this.stats = stats;
  }

  @Override
  public Iterator<Feature> iterator() {
    return output.iterator();
  }

  public Feature geometry(String layer, Geometry geometry) {
    Feature feature = new Feature(layer, geometry, source.id());
    output.add(feature);
    return feature;
  }

  public Feature point(String layer) {
    try {
      if (!source.isPoint()) {
        throw new GeometryException("feature_not_point", "not a point");
      }
      return geometry(layer, source.worldGeometry());
    } catch (GeometryException e) {
      e.log(stats, "feature_point", "Error getting point geometry for " + source.id());
      return new Feature(layer, EMPTY_GEOM, source.id());
    }
  }

  public Feature centroid(String layer) {
    try {
      return geometry(layer, source.centroid());
    } catch (GeometryException e) {
      e.log(stats, "feature_centroid", "Error getting centroid for " + source.id());
      return new Feature(layer, EMPTY_GEOM, source.id());
    }
  }

  public Feature line(String layer) {
    try {
      return geometry(layer, source.line());
    } catch (GeometryException e) {
      e.log(stats, "feature_line", "Error constructing line for " + source.id());
      return new Feature(layer, EMPTY_GEOM, source.id());
    }
  }

  public Feature polygon(String layer) {
    try {
      return geometry(layer, source.polygon());
    } catch (GeometryException e) {
      e.log(stats, "feature_polygon", "Error constructing polygon for " + source.id());
      return new Feature(layer, EMPTY_GEOM, source.id());
    }
  }

  public Feature centroidIfConvex(String layer) {
    try {
      return geometry(layer, source.centroidIfConvex());
    } catch (GeometryException e) {
      e.log(stats, "feature_centroid_if_convex", "Error constructing centroid if convex for " + source.id());
      return new Feature(layer, EMPTY_GEOM, source.id());
    }
  }

  public Feature pointOnSurface(String layer) {
    try {
      return geometry(layer, source.pointOnSurface());
    } catch (GeometryException e) {
      e.log(stats, "feature_point_on_surface", "Error constructing point on surface for " + source.id());
      return new Feature(layer, EMPTY_GEOM, source.id());
    }
  }

  public static record Factory(CommonParams config, Stats stats) {

    public FeatureCollector get(SourceFeature source) {
      return new FeatureCollector(source, config, stats);
    }
  }

  public final class Feature {

    private static final double DEFAULT_LABEL_GRID_SIZE = 0;
    private static final int DEFAULT_LABEL_GRID_LIMIT = 0;
    private final String layer;
    private final Geometry geom;
    private final Map<String, Object> attrs = new TreeMap<>();
    private final GeometryType geometryType;
    private final long sourceId;
    private int zOrder;
    private int minzoom = config.minzoom();
    private int maxzoom = config.maxzoom();
    private double defaultBufferPixels = 4;
    private ZoomFunction<Number> bufferPixelOverrides;
    private double defaultMinPixelSize = 1;
    private double minPixelSizeAtMaxZoom = 256d / 4096;
    private ZoomFunction<Number> minPixelSize = null;
    private ZoomFunction<Number> labelGridPixelSize = null;
    private ZoomFunction<Number> labelGridLimit = null;
    private boolean attrsChangeByZoom = false;
    private CacheByZoom<Map<String, Object>> attrCache = null;
    private double defaultPixelTolerance = 0.1d;
    private double pixelToleranceAtMaxZoom = 256d / 4096;
    private ZoomFunction<Double> pixelTolerance = null;
    private String numPointsAttr = null;

    private Feature(String layer, Geometry geom, long sourceId) {
      this.layer = layer;
      this.geom = geom;
      this.zOrder = 0;
      this.geometryType = GeometryType.valueOf(geom);
      this.sourceId = sourceId;
    }

    public long sourceId() {
      return sourceId;
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
      return zoom == 14 ? minPixelSizeAtMaxZoom
        : ZoomFunction.applyAsDoubleOrElse(minPixelSize, zoom, defaultMinPixelSize);
    }

    public Feature setMinPixelSize(double minPixelSize) {
      this.defaultMinPixelSize = minPixelSize;
      return this;
    }

    public Feature setMinPixelSizeThresholds(ZoomFunction<Number> levels) {
      this.minPixelSize = levels;
      return this;
    }

    public Feature setMinPixelSizeBelowZoom(int zoom, double minPixelSize) {
      this.minPixelSize = ZoomFunction.maxZoom(zoom, minPixelSize);
      return this;
    }

    public Feature setMinPixelSizeAtMaxZoom(double minPixelSize) {
      this.minPixelSizeAtMaxZoom = minPixelSize;
      return this;
    }

    public Feature setMinPixelSizeAtAllZooms(int minPixelSize) {
      return setMinPixelSizeAtMaxZoom(minPixelSize)
        .setMinPixelSize(minPixelSize);
    }


    public Feature setPixelTolerance(double tolerance) {
      this.defaultPixelTolerance = tolerance;
      return this;
    }

    public Feature setPixelToleranceAtMaxZoom(double tolerance) {
      this.pixelToleranceAtMaxZoom = tolerance;
      return this;
    }

    public Feature setPixelToleranceAtAllZooms(double tolerance) {
      return setPixelToleranceAtMaxZoom(tolerance).setPixelTolerance(tolerance);
    }

    public Feature setPixelToleranceBelowZoom(int zoom, double tolerance) {
      this.pixelTolerance = ZoomFunction.maxZoom(zoom, tolerance);
      return this;
    }

    public double getPixelTolerance(int zoom) {
      return zoom == 14 ? pixelToleranceAtMaxZoom
        : ZoomFunction.applyAsDoubleOrElse(pixelTolerance, zoom, defaultPixelTolerance);
    }

    public double getLabelGridPixelSizeAtZoom(int zoom) {
      return ZoomFunction.applyAsDoubleOrElse(labelGridPixelSize, zoom, DEFAULT_LABEL_GRID_SIZE);
    }

    public int getLabelGridLimitAtZoom(int zoom) {
      return ZoomFunction.applyAsIntOrElse(labelGridLimit, zoom, DEFAULT_LABEL_GRID_LIMIT);
    }

    public Feature setLabelGridPixelSizeFunction(ZoomFunction<Number> labelGridSize) {
      this.labelGridPixelSize = labelGridSize;
      return this;
    }

    public Feature setLabelGridLimitFunction(ZoomFunction<Number> labelGridLimit) {
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

    private Map<String, Object> computeAttrsAtZoom(int zoom) {
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

    public Map<String, Object> getAttrsAtZoom(int zoom) {
      if (!attrsChangeByZoom) {
        return attrs;
      }
      if (attrCache == null) {
        attrCache = CacheByZoom.create(config, this::computeAttrsAtZoom);
      }
      return attrCache.get(zoom);
    }

    public Feature inheritFromSource(String attr) {
      return setAttr(attr, source.getTag(attr));
    }

    public Feature setAttr(String key, Object value) {
      if (value instanceof ZoomFunction) {
        attrsChangeByZoom = true;
      }
      if (value != null) {
        attrs.put(key, value);
      }
      return this;
    }

    public Feature setAttrWithMinzoom(String key, Object value, int minzoom) {
      return setAttr(key, ZoomFunction.minZoom(minzoom, value));
    }

    public GeometryType getGeometryType() {
      return geometryType;
    }

    public boolean area() {
      return geometryType == GeometryType.POLYGON;
    }

    @Override
    public String toString() {
      return "Feature{" +
        "layer='" + layer + '\'' +
        ", geom=" + geom.getGeometryType() +
        ", attrs=" + attrs +
        '}';
    }

    public Feature setAttrs(Map<String, Object> names) {
      attrs.putAll(names);
      return this;
    }

    public Feature setNumPointsAttr(String numPointsAttr) {
      this.numPointsAttr = numPointsAttr;
      return this;
    }

    public String getNumPointsAttr() {
      return numPointsAttr;
    }
  }
}
