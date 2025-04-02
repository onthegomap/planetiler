package com.onthegomap.planetiler;

import com.google.common.collect.Range;
import com.onthegomap.planetiler.collection.FeatureGroup;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.geo.GeometryPipeline;
import com.onthegomap.planetiler.geo.GeometryType;
import com.onthegomap.planetiler.geo.SimplifyMethod;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.reader.Struct;
import com.onthegomap.planetiler.render.FeatureRenderer;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.util.CacheByZoom;
import com.onthegomap.planetiler.util.MapUtil;
import com.onthegomap.planetiler.util.MergingRangeMap;
import com.onthegomap.planetiler.util.ZoomFunction;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.locationtech.jts.algorithm.construct.MaximumInscribedCircle;
import org.locationtech.jts.geom.Geometry;

/**
 * Utility that {@link Profile} implementations use to build map features that should be emitted for an input source
 * feature.
 * <p>
 * For example to add a polygon feature for a lake and a center label point with its name:
 * {@snippet :
 * FeatureCollector featureCollector;
 * featureCollector.polygon("water")
 *   .setAttr("class", "lake");
 * featureCollector.centroid("water_name")
 *   .setAttr("class", "lake")
 *   .setAttr("name", element.getString("name"));
 * }
 */
public class FeatureCollector implements Iterable<FeatureCollector.Feature> {

  private static final Geometry EMPTY_GEOM = GeoUtils.JTS_FACTORY.createGeometryCollection();

  private final SourceFeature source;
  private final List<Feature> output = new ArrayList<>();
  private final PlanetilerConfig config;
  private final Stats stats;

  private FeatureCollector(SourceFeature source, PlanetilerConfig config, Stats stats) {
    this.source = source;
    this.config = config;
    this.stats = stats;
  }

  @Override
  public Iterator<Feature> iterator() {
    return output.iterator();
  }

  /**
   * Starts building a new map feature with an explicit JTS {@code geometry} that overrides the source geometry.
   *
   * @param layer    the output vector tile layer this feature will be written to
   * @param geometry the explicit geometry to use instead of what is present in source data
   * @return a feature that can be configured further.
   */
  public Feature geometry(String layer, Geometry geometry) {
    // TODO args could also provide a list of source IDs to put into slot 4, 5, 6, etc..
    // to differentiate between other sources besides just OSM and "other"
    long vectorTileId = config.featureSourceIdMultiplier() < 4 ? source.id() :
      source.vectorTileFeatureId(config.featureSourceIdMultiplier());
    Feature feature = new Feature(layer, geometry, vectorTileId);
    output.add(feature);
    return feature;
  }

  /**
   * Starts building a new point map feature that expects the source feature to be a point.
   * <p>
   * If the source feature is not a point, logs an error and returns a feature that can be configured, but won't
   * actually emit anything to the map.
   *
   * @param layer the output vector tile layer this feature will be written to
   * @return a feature that can be configured further.
   */
  public Feature point(String layer) {
    try {
      if (!source.isPoint()) {
        throw new GeometryException("feature_not_point", "not a point", true);
      }
      return geometry(layer, source.worldGeometry());
    } catch (GeometryException e) {
      e.log(stats, "feature_point", "Error getting point geometry for " + source);
      return empty(layer);
    }
  }

  /**
   * Starts building a new line map feature that expects the source feature to be a line.
   * <p>
   * If the source feature cannot be a line, logs an error and returns a feature that can be configured, but won't
   * actually emit anything to the map.
   * <p>
   * Some OSM closed OSM ways can be both a polygon and a line
   *
   * @param layer the output vector tile layer this feature will be written to
   * @return a feature that can be configured further.
   */
  public Feature line(String layer) {
    try {
      return geometry(layer, source.line());
    } catch (GeometryException e) {
      e.log(stats, "feature_line", "Error constructing line for " + source);
      return empty(layer);
    }
  }


  /**
   * Starts building a new partial line feature from {@code start} to {@code end} where 0 is the beginning of the line
   * and 1 is the end of the line.
   * <p>
   * If the source feature cannot be a line, logs an error and returns a feature that can be configured, but won't
   * actually emit anything to the map.
   *
   * @param layer the output vector tile layer this feature will be written to
   * @return a feature that can be configured further.
   */
  public Feature partialLine(String layer, double start, double end) {
    try {
      return geometry(layer, source.partialLine(start, end));
    } catch (GeometryException e) {
      e.log(stats, "feature_partial_line", "Error constructing partial line for " + source);
      return empty(layer);
    }
  }

  /**
   * Starts building a new polygon map feature that expects the source feature to be a polygon.
   * <p>
   * If the source feature cannot be a polygon, logs an error and returns a feature that can be configured, but won't
   * actually emit anything to the map.
   * <p>
   * Some OSM closed OSM ways can be both a polygon and a line
   *
   * @param layer the output vector tile layer this feature will be written to
   * @return a feature that can be configured further.
   */
  public Feature polygon(String layer) {
    try {
      return geometry(layer, source.polygon());
    } catch (GeometryException e) {
      e.log(stats, "feature_polygon", "Error constructing polygon for " + source);
      return empty(layer);
    }
  }

  /**
   * Starts building a new polygon, line, or point map feature based on the geometry type of the input feature.
   *
   * @param layer the output vector tile layer this feature will be written to
   * @return a feature that can be configured further.
   */
  public Feature anyGeometry(String layer) {
    return source.canBePolygon() ? polygon(layer) :
      source.canBeLine() ? line(layer) :
      source.isPoint() ? point(layer) :
      empty(layer);
  }

  private Feature empty(String layer) {
    return new Feature(layer, EMPTY_GEOM, source.id());
  }

  /**
   * Starts building a new point map feature with geometry from {@link Geometry#getCentroid()} of the source feature.
   *
   * @param layer the output vector tile layer this feature will be written to
   * @return a feature that can be configured further.
   */
  public Feature centroid(String layer) {
    try {
      return geometry(layer, source.centroid());
    } catch (GeometryException e) {
      e.log(stats, "feature_centroid", "Error getting centroid for " + source);
      return empty(layer);
    }
  }

  /**
   * Starts building a new point map feature with geometry from {@link Geometry#getCentroid()} if the source feature is
   * a point, line, or simple convex polygon, or {@link Geometry#getInteriorPoint()} if it is a multipolygon, polygon
   * with holes, or concave simple polygon.
   *
   * @param layer the output vector tile layer this feature will be written to
   * @return a feature that can be configured further.
   */
  public Feature centroidIfConvex(String layer) {
    try {
      return geometry(layer, source.centroidIfConvex());
    } catch (GeometryException e) {
      e.log(stats, "feature_centroid_if_convex", "Error constructing centroid if convex for " + source);
      return empty(layer);
    }
  }

  /**
   * Starts building a new point map feature with geometry from {@link Geometry#getInteriorPoint()} of the source
   * feature.
   *
   * @param layer the output vector tile layer this feature will be written to
   * @return a feature that can be configured further.
   */
  public Feature pointOnSurface(String layer) {
    try {
      return geometry(layer, source.pointOnSurface());
    } catch (GeometryException e) {
      e.log(stats, "feature_point_on_surface", "Error constructing point on surface for " + source);
      return empty(layer);
    }
  }

  /**
   * Starts building a new point map feature at the furthest interior point of a polygon from its edge using
   * {@link MaximumInscribedCircle} (aka "pole of inaccessibility") of the source feature.
   * <p>
   * NOTE: This is substantially more expensive to compute than {@link #centroid(String)} or
   * {@link #pointOnSurface(String)}, especially for small {@code tolerance} values.
   *
   * @param layer     the output vector tile layer this feature will be written to
   * @param tolerance precision for calculating maximum inscribed circle. 0.01 means 1% of the square root of the area.
   *                  Smaller values for a more precise tolerance become very expensive to compute. Values between 5%
   *                  and 10% are a good compromise of performance vs. precision.
   * @return a feature that can be configured further.
   */
  public Feature innermostPoint(String layer, double tolerance) {
    try {
      return geometry(layer, source.innermostPoint(tolerance));
    } catch (GeometryException e) {
      e.log(stats, "feature_innermost_point", "Error constructing innermost point for " + source);
      return empty(layer);
    }
  }

  /** Alias for {@link #innermostPoint(String, double)} with a default tolerance of 10%. */
  public Feature innermostPoint(String layer) {
    return innermostPoint(layer, 0.1);
  }


  /**
   * Starts building a new point map feature at the midpoint of this line, or the longest line segment if a
   * multilinestring.
   *
   * @param layer the output vector tile layer this feature will be written to
   * @return a feature that can be configured further.
   */
  public Feature lineMidpoint(String layer) {
    try {
      return geometry(layer, source.lineMidpoint());
    } catch (GeometryException e) {
      e.log(stats, "feature_line_midpoint", "Error getting midpoint for " + source);
      return empty(layer);
    }
  }

  /**
   * Starts building a new point map feature at a certain ratio along the linestring or longest segment if it is a
   * multilinestring.
   *
   * @param layer the output vector tile layer this feature will be written to
   * @param ratio the ratio along the line: 0 for start, 1 for end, 0.5 for midpoint
   * @return a feature that can be configured further.
   */
  public Feature pointAlongLine(String layer, double ratio) {
    try {
      return geometry(layer, source.pointAlongLine(ratio));
    } catch (GeometryException e) {
      e.log(stats, "feature_point_along_line", "Error getting point along line for " + source);
      return empty(layer);
    }
  }

  /** Returns the minimum zoom level at which this feature is at least {@code pixelSize} pixels large. */
  public int getMinZoomForPixelSize(double pixelSize) {
    try {
      return GeoUtils.minZoomForPixelSize(source.size(), pixelSize);
    } catch (GeometryException e) {
      e.log(stats, "min_zoom_for_size_failure", "Error getting min zoom for size from geometry " + source);
      return config.maxzoom();
    }
  }


  /** Returns the actual pixel size of the source feature at {@code zoom} (length if line, sqrt(area) if polygon). */
  public double getPixelSizeAtZoom(int zoom) {
    try {
      return source.size() * (256 << zoom);
    } catch (GeometryException e) {
      e.log(stats, "source_feature_pixel_size_at_zoom_failure",
        "Error getting source feature pixel size at zoom from geometry " + source);
      return 0;
    }
  }

  private sealed interface OverrideCommand {
    Range<Double> range();
  }
  private record Minzoom(Range<Double> range, int minzoom) implements OverrideCommand {}
  private record Maxzoom(Range<Double> range, int maxzoom) implements OverrideCommand {}
  private record Omit(Range<Double> range) implements OverrideCommand {}
  private record Attr(Range<Double> range, String key, Object value) implements OverrideCommand {}

  public interface WithZoomRange<T extends WithZoomRange<T>> {

    /**
     * Sets the zoom range (inclusive) that this feature appears in.
     * <p>
     * If not called, then defaults to all zoom levels.
     */
    default T setZoomRange(int min, int max) {
      assert min <= max;
      return setMinZoom(min).setMaxZoom(max);
    }


    /**
     * Sets the minimum zoom level (inclusive) that this feature appears in.
     * <p>
     * If not called, defaults to minimum zoom-level of the map.
     */
    T setMinZoom(int min);

    /**
     * Sets the maximum zoom level (inclusive) that this feature appears in.
     * <p>
     * If not called, defaults to maximum zoom-level of the map.
     */
    T setMaxZoom(int max);
  }

  public interface WithSelf<T extends WithSelf<T>> {

    default T self() {
      return (T) this;
    }
  }

  public interface WithAttrs<T extends WithAttrs<T>> extends WithSelf<T> {

    /** Copies the value for {@code key} attribute from source feature to the output feature. */
    default T inheritAttrFromSource(String key) {
      return setAttr(key, collector().source.getTag(key));
    }

    /** Copies the values for {@code keys} attributes from source feature to the output feature. */
    default T inheritAttrsFromSource(String... keys) {
      for (var key : keys) {
        inheritAttrFromSource(key);
      }
      return self();
    }


    /** Copies the values for {@code keys} attributes from source feature to the output feature. */
    default T inheritAttrsFromSourceWithMinzoom(int minzoom, String... keys) {
      for (var key : keys) {
        setAttrWithMinzoom(key, collector().source.getTag(key), minzoom);
      }
      return self();
    }

    /**
     * Sets an attribute on the output feature to either a string, number, boolean, or instance of {@link ZoomFunction}
     * to change the value for {@code key} by zoom-level.
     */
    T setAttr(String key, Object value);

    /**
     * Sets the value for {@code key} attribute at or above {@code minzoom}. Below {@code minzoom} it will be ignored.
     * <p>
     * Replaces all previous value that has been for {@code key} at any zoom level. To have a value that changes at
     * multiple zoom level thresholds, call {@link #setAttr(String, Object)} with a manually-constructed
     * {@link ZoomFunction} value.
     */
    default T setAttrWithMinzoom(String key, Object value, int minzoom) {
      return setAttr(key, ZoomFunction.minZoom(minzoom, value));
    }

    /**
     * Sets the value for {@code key} only at zoom levels where the feature is at least {@code minPixelSize} pixels in
     * size.
     */
    default T setAttrWithMinSize(String key, Object value, double minPixelSize) {
      return setAttrWithMinzoom(key, value, getMinZoomForPixelSize(minPixelSize));
    }

    /**
     * Sets the value for {@code key} so that it always shows when {@code zoom_level >= minZoomToShowAlways} but only
     * shows when {@code minZoomIfBigEnough <= zoom_level < minZoomToShowAlways} when it is at least
     * {@code minPixelSize} pixels in size.
     * <p>
     * If you need more flexibility, use {@link #getMinZoomForPixelSize(double)} directly, or create a
     * {@link ZoomFunction} that calculates {@link #getPixelSizeAtZoom(int)} and applies a custom threshold based on the
     * zoom level.
     */
    default T setAttrWithMinSize(String key, Object value, double minPixelSize, int minZoomIfBigEnough,
      int minZoomToShowAlways) {
      return setAttrWithMinzoom(key, value,
        Math.clamp(getMinZoomForPixelSize(minPixelSize), minZoomIfBigEnough, minZoomToShowAlways));
    }

    default int getMinZoomForPixelSize(double minPixelSize) {
      return collector().getMinZoomForPixelSize(minPixelSize);
    }

    /**
     * Inserts all key/value pairs in {@code attrs} into the set of attribute to emit on the output feature at or above
     * {@code minzoom}.
     * <p>
     * Replace values that have already been set.
     */
    default T putAttrsWithMinzoom(Map<String, Object> attrs, int minzoom) {
      for (var entry : attrs.entrySet()) {
        setAttrWithMinzoom(entry.getKey(), entry.getValue(), minzoom);
      }
      return self();
    }

    /**
     * Inserts all key/value pairs in {@code attrs} into the set of attribute to emit on the output feature.
     * <p>
     * Does not touch attributes that have already been set.
     * <p>
     * Values in {@code attrs} can either be the raw value to set, or an instance of {@link ZoomFunction} to change the
     * value for that attribute by zoom level.
     */
    default T putAttrs(Map<String, Object> attrs) {
      for (var entry : attrs.entrySet()) {
        setAttr(entry.getKey(), entry.getValue());
      }
      return self();
    }

    /** Returns the {@link FeatureCollector} this feature came from. */
    FeatureCollector collector();
  }

  /**
   * Creates new feature collector instances for each source feature that we encounter.
   */
  public record Factory(PlanetilerConfig config, Stats stats) {

    public FeatureCollector get(SourceFeature source) {
      return new FeatureCollector(source, config, stats);
    }
  }

  private record PartialOverride(Range<Double> range, Object key, Object value) {}

  /** A fully-configured subset of this line feature with linear-scoped attributes applied to a subset of the range.. */
  public record RangeWithTags(double start, double end, Geometry geom, Map<String, Object> attrs) {}

  /**
   * A builder for an output map feature that contains all the information that will be needed to render vector tile
   * features from the input element.
   * <p>
   * Some feature attributes are set globally (like sort key), and some allow the value to change by zoom-level (like
   * tags).
   */
  public final class Feature implements WithZoomRange<Feature>, WithAttrs<Feature> {

    private static final double DEFAULT_LABEL_GRID_SIZE = 0;
    private static final int DEFAULT_LABEL_GRID_LIMIT = 0;

    private final String layer;
    private final Geometry geom;
    private final Map<String, Object> attrs = new TreeMap<>();
    private final GeometryType geometryType;
    private long id;

    private int sortKey = 0;

    private int minzoom = config.minzoom();
    private int maxzoom = config.maxzoom();

    private ZoomFunction<Number> labelGridPixelSize = null;
    private ZoomFunction<Number> labelGridLimit = null;

    private boolean mustUnwrapValues = false;
    private CacheByZoom<Map<String, Object>> attrCache = null;
    private CacheByZoom<List<RangeWithTags>> partialRangeCache = null;

    private double defaultBufferPixels = 4;
    private ZoomFunction<Number> bufferPixelOverrides;

    // TODO better API for default value, value at max zoom, and zoom-specific overrides for tolerance and min size?
    private double defaultMinPixelSize = config.minFeatureSizeBelowMaxZoom();
    private double minPixelSizeAtMaxZoom = config.minFeatureSizeAtMaxZoom();
    private ZoomFunction<Number> minPixelSize = null;

    private double defaultPixelTolerance = config.simplifyToleranceBelowMaxZoom();
    private double pixelToleranceAtMaxZoom = config.simplifyToleranceAtMaxZoom();
    private ZoomFunction<Number> pixelTolerance = null;

    private SimplifyMethod defaultSimplifyMethod = SimplifyMethod.DOUGLAS_PEUCKER;
    private ZoomFunction<SimplifyMethod> simplifyMethod = null;

    private GeometryPipeline defaultGeometryPipeline = null;
    private ZoomFunction<GeometryPipeline> geometryPipelineByZoom = null;

    private String numPointsAttr = null;
    private List<OverrideCommand> partialOverrides = null;

    private Feature(String layer, Geometry geom, long id) {
      this.layer = layer;
      this.geom = geom;
      this.geometryType = GeometryType.typeOf(geom);
      this.id = id;
      if (geometryType == GeometryType.POINT) {
        minPixelSizeAtMaxZoom = 0;
        defaultMinPixelSize = 0;
      }
    }

    /** Returns the original ID of the source feature that this feature came from (i.e. OSM node/way ID). */
    public long getId() {
      return id;
    }

    public Feature setId(long id) {
      this.id = id;
      return this;
    }

    GeometryType getGeometryType() {
      return geometryType;
    }

    public boolean isPolygon() {
      return geometryType == GeometryType.POLYGON;
    }

    /**
     * Returns the value by which features are sorted within a layer in the output vector tile.
     */
    public int getSortKey() {
      return sortKey;
    }

    /**
     * Sets the value by which features are sorted within a layer in the output vector tile. Sort key gets packed into
     * {@link FeatureGroup#SORT_KEY_BITS} bits so the range of this is limited to {@code -(2^(bits-1))} to
     * {@code (2^(bits-1))-1}.
     * <p>
     * Circles, lines, and polygons are rendered in the order they appear in each layer, so features that appear later
     * (higher sort key) show up on top of features with a lower sort key.
     * <p>
     * For symbols (text/icons) where clients try to avoid label collisions, features are placed in the order they
     * appear in each layer, so features that appear earlier (lower sort key) will show up at lower zoom levels than
     * feature that appear later (higher sort key) in a layer.
     */
    public Feature setSortKey(int sortKey) {
      assert sortKey >= FeatureGroup.SORT_KEY_MIN && sortKey <= FeatureGroup.SORT_KEY_MAX : "Sort key " + sortKey +
        " outside of allowed range [" + FeatureGroup.SORT_KEY_MIN + ", " + FeatureGroup.SORT_KEY_MAX + "]";
      this.sortKey = sortKey;
      return this;
    }

    /** Sets the value by which features are sorted from high to low within a layer in the output vector tile. */
    public Feature setSortKeyDescending(int sortKey) {
      return setSortKey(FeatureGroup.SORT_KEY_MAX + FeatureGroup.SORT_KEY_MIN - sortKey);
    }

    /** Returns the minimum zoom level (inclusive) that this feature appears in. */
    public int getMinZoom() {
      return minzoom;
    }

    @Override
    public Feature setMinZoom(int min) {
      minzoom = Math.max(min, config.minzoom());
      return this;
    }

    /** Returns the maximum zoom level (inclusive) that this feature appears in. */
    public int getMaxZoom() {
      return maxzoom;
    }

    @Override
    public Feature setMaxZoom(int max) {
      maxzoom = Math.min(max, config.maxzoom());
      return this;
    }

    /** Returns the output vector tile layer that this feature will appear in. */
    public String getLayer() {
      return layer;
    }

    /**
     * Returns the JTS geometry (in world web mercator coordinates) of this feature.
     * <p>
     * Subsequent postprocessing in {@link FeatureRenderer} will slice this into tile geometries.
     */
    public Geometry getGeometry() {
      return geom;
    }

    /** Returns the number of pixels of detail to render outside the visible tile boundary at {@code zoom}. */
    public double getBufferPixelsAtZoom(int zoom) {
      return ZoomFunction.applyAsDoubleOrElse(bufferPixelOverrides, zoom, defaultBufferPixels);
    }

    /**
     * Sets the default number of pixels of detail to render outside the visible tile boundary when no zoom-specific
     * override is set in {@link #setBufferPixelOverrides(ZoomFunction)}.
     */
    public Feature setBufferPixels(double buffer) {
      defaultBufferPixels = buffer;
      return this;
    }

    /**
     * Sets zoom-specific overrides to the number of pixels of detail to render outside the visible tile boundary.
     * <p>
     * If {@code buffer} is {@code null} or returns {@code null}, the buffer pixels will default to
     * {@link #setBufferPixels(double)}.
     */
    public Feature setBufferPixelOverrides(ZoomFunction<Number> buffer) {
      bufferPixelOverrides = buffer;
      return this;
    }


    /**
     * Returns the minimum resolution in tile pixels of features to emit at {@code zoom}.
     * <p>
     * For line features, this is length, and for polygon features this is the square root of the minimum area of
     * features to emit.
     */
    public double getMinPixelSizeAtZoom(int zoom) {
      return zoom == config.maxzoomForRendering() ? minPixelSizeAtMaxZoom :
        ZoomFunction.applyAsDoubleOrElse(minPixelSize, zoom, defaultMinPixelSize);
    }

    /**
     * Sets the minimum length of line features or square root of the minimum area of polygon features to emit below the
     * maximum zoom-level of the map.
     * <p>
     * At the maximum zoom level of the map, clients can "overzoom" in on features, so this leaves the minimum size at
     * the max zoom level at {@link PlanetilerConfig#minFeatureSizeAtMaxZoom()} unless you explicitly override it with
     * {@link #setMinPixelSizeAtMaxZoom(double)} or {@link #setMinPixelSizeAtAllZooms(int)}.
     */
    public Feature setMinPixelSize(double minPixelSize) {
      this.defaultMinPixelSize = minPixelSize;
      return this;
    }

    /**
     * Sets zoom-specific overrides to the minimum length of line features or square root of the minimum area of polygon
     * features to emit below the maximum zoom-level of the map.
     * <p>
     * At the maximum zoom level of the map, clients can "overzoom" in on features, so this leaves the minimum size at
     * the max zoom level at {@link PlanetilerConfig#minFeatureSizeAtMaxZoom()} unless you explicitly override it with
     * {@link #setMinPixelSizeAtMaxZoom(double)} or {@link #setMinPixelSizeAtAllZooms(int)}.
     * <p>
     * If {@code levels} is {@code null} or returns {@code null}, the min pixel size will default to the default value.
     */
    public Feature setMinPixelSizeOverrides(ZoomFunction<Number> levels) {
      this.minPixelSize = levels;
      return this;
    }

    /**
     * Overrides the default minimum pixel size at and below {@code zoom} with {@code minPixelSize}.
     * <p>
     * This replaces all previous zoom overrides that were set. To use multiple zoom-level thresholds, create a
     * {@link ZoomFunction} explicitly and pass it to {@link #setMinPixelSizeOverrides(ZoomFunction)}.
     */
    public Feature setMinPixelSizeBelowZoom(int zoom, double minPixelSize) {
      if (zoom >= config.maxzoomForRendering()) {
        minPixelSizeAtMaxZoom = minPixelSize;
      }
      this.minPixelSize = ZoomFunction.maxZoom(zoom, minPixelSize);
      return this;
    }

    /**
     * Sets the minimum length of line features or square root of the minimum area of polygon features to emit at the
     * maximum zoom-level of the map.
     * <p>
     * Since clients can "overzoom" in on features past the maximum zoom level, this is typically much smaller than min
     * pixel size at lower zoom levels.
     * <p>
     * This overrides, but does not replace the default min pixel size or overrides set through other methods.
     */
    public Feature setMinPixelSizeAtMaxZoom(double minPixelSize) {
      this.minPixelSizeAtMaxZoom = minPixelSize;
      return this;
    }

    /**
     * Sets the minimum length of line features or square root of the minimum area of polygon features to emit at all
     * zoom levels, including the maximum zoom-level of the map.
     * <p>
     * This replaces previous default values, but not overrides set with
     * {@link #setMinPixelSizeOverrides(ZoomFunction)}.
     */
    public Feature setMinPixelSizeAtAllZooms(int minPixelSize) {
      this.minPixelSizeAtMaxZoom = minPixelSize;
      return this.setMinPixelSize(minPixelSize);
    }

    /**
     * Returns the simplification tolerance for lines and polygons in tile pixels at {@code zoom}.
     */
    public double getPixelToleranceAtZoom(int zoom) {
      return zoom == config.maxzoomForRendering() ? pixelToleranceAtMaxZoom :
        ZoomFunction.applyAsDoubleOrElse(pixelTolerance, zoom, defaultPixelTolerance);
    }

    /**
     * Sets the fallback line and polygon simplify method when not overriden by *
     * {@link #setSimplifyMethodOverrides(ZoomFunction)}.
     */
    public FeatureCollector.Feature setSimplifyMethod(SimplifyMethod strategy) {
      defaultSimplifyMethod = strategy;
      return this;
    }

    /** Set simplification algorithm to use at different zoom levels. */
    public FeatureCollector.Feature setSimplifyMethodOverrides(ZoomFunction<SimplifyMethod> overrides) {
      simplifyMethod = overrides;
      return this;
    }

    /**
     * Returns the simplification method for lines and polygons in tile pixels at {@code zoom}.
     */
    public SimplifyMethod getSimplifyMethodAtZoom(int zoom) {
      return ZoomFunction.applyOrElse(simplifyMethod, zoom, defaultSimplifyMethod);
    }

    /**
     * Sets the default pipeline to apply to geometries scaled to tile coordinates right before emitting vector tile
     * features. This function gets run instead of simplification, so should include any simplification if you want
     * that.
     * <p>
     * Geometries will be in scaled tile coordinates, so {@code 0,0} is the northwest corner and {@code 2^z, 2^z} is the
     * southeast corner of the world scaled to web mercator coordinates.
     */
    public Feature transformScaledGeometry(GeometryPipeline pipeline) {
      this.defaultGeometryPipeline = pipeline;
      return this;
    }

    /**
     * Dynamically change the geometry pipeline to apply to geometries scaled to tile coordinates right before emitting
     * vector tile features at each zoom level. These functions get run instead of simplification, so should include any
     * simplification if you want that.
     * <p>
     * Geometries will be in scaled tile coordinates, so {@code 0,0} is the northwest corner and {@code 2^z, 2^z} is the
     * southeast corner of the world scaled to web mercator coordinates.
     */
    public Feature transformScaledGeometryByZoom(ZoomFunction<GeometryPipeline> overrides) {
      this.geometryPipelineByZoom = overrides;
      return this;
    }

    /**
     * Returns the geometry transform function to apply to scaled geometries at {@code zoom}, or null to not update them
     * at all.
     */
    public GeometryPipeline getScaledGeometryTransformAtZoom(int zoom) {
      return ZoomFunction.applyOrElse(geometryPipelineByZoom, zoom, defaultGeometryPipeline);
    }

    /**
     * Sets the simplification tolerance for lines and polygons in tile pixels below the maximum zoom-level of the map.
     * <p>
     * Since clients can "overzoom" past the max zoom of the map, this is typically smaller than the default tolerance
     * to provide more detail as you zoom in.
     * <p>
     * This does not replace any overrides that were set with {@link #setPixelToleranceOverrides(ZoomFunction)}.
     */
    public Feature setPixelTolerance(double tolerance) {
      this.defaultPixelTolerance = tolerance;
      return this;
    }

    /**
     * Sets the simplification tolerance for lines and polygons in tile pixels at the maximum zoom-level of the map.
     * <p>
     * This does not replace the default value at other zoom levels set through {@link #setPixelTolerance(double)} any
     * zoom-specific overrides that were set with {@link #setPixelToleranceOverrides(ZoomFunction)}.
     */
    public Feature setPixelToleranceAtMaxZoom(double tolerance) {
      this.pixelToleranceAtMaxZoom = tolerance;
      return this;
    }

    /**
     * Sets the simplification tolerance for lines and polygons in tile pixels including at the maximum zoom-level of
     * the map.
     * <p>
     * This does not replace the default value at other zoom levels set through {@link #setPixelTolerance(double)}.
     */
    public Feature setPixelToleranceAtAllZooms(double tolerance) {
      return setPixelToleranceAtMaxZoom(tolerance).setPixelTolerance(tolerance);
    }

    /**
     * Sets zoom-specific overrides to the simplification tolerance for lines and polygons in tile pixels below the
     * maximum zoom-level of the map.
     * <p>
     * At the maximum zoom level of the map, clients can "overzoom" in on features, so this leaves the tolerance at the
     * max zoom level set to {@link PlanetilerConfig#simplifyToleranceAtMaxZoom()} unless you explicitly override it
     * with {@link #setMinPixelSizeAtAllZooms(int)} or {@link #setMinPixelSizeAtMaxZoom(double)}.
     * <p>
     * If {@code levels} is {@code null} or returns {@code null}, the min pixel size will default to the default value.
     */
    public Feature setPixelToleranceOverrides(ZoomFunction<Number> overrides) {
      this.pixelTolerance = overrides;
      return this;
    }

    /**
     * Overrides the default simplification tolerance for lines and polygons in tile pixels at and below {@code zoom}
     * with {@code minPixelSize}.
     * <p>
     * This replaces all previous zoom overrides that were set. To use multiple zoom-level thresholds, create a
     * {@link ZoomFunction} explicitly and pass it to {@link #setPixelToleranceOverrides(ZoomFunction)}
     */
    public Feature setPixelToleranceBelowZoom(int zoom, double tolerance) {
      if (zoom == config.maxzoomForRendering()) {
        pixelToleranceAtMaxZoom = tolerance;
      }
      return setPixelToleranceOverrides(ZoomFunction.maxZoom(zoom, tolerance));
    }

    public boolean hasLabelGrid() {
      return labelGridPixelSize != null || labelGridLimit != null;
    }

    /**
     * Returns the size in pixels of the grid used to group or limit output points.
     *
     * @throws AssertionError when assertions are enabled and the returned value is smaller than the buffer pixel size
     */
    public double getPointLabelGridPixelSizeAtZoom(int zoom) {
      double result = ZoomFunction.applyAsDoubleOrElse(labelGridPixelSize, zoom, DEFAULT_LABEL_GRID_SIZE);
      // TODO is this enough? what about a grid square that ends just before the start of the tile
      assert result <= getBufferPixelsAtZoom(
        zoom) : "to avoid inconsistent rendering of the same point between adjacent tiles, buffer pixel size should be >= label grid size but in '%s' buffer pixel size=%f was less than label grid size=%f"
          .formatted(
            getLayer(), getBufferPixelsAtZoom(zoom), result);
      return result;
    }

    /**
     * Returns the maximum number of lowest-sort-key points to include in the output vector tile in each square of a
     * grid with size {@link #getPointLabelGridPixelSizeAtZoom(int)}.
     */
    public int getPointLabelGridLimitAtZoom(int zoom) {
      return ZoomFunction.applyAsIntOrElse(labelGridLimit, zoom, DEFAULT_LABEL_GRID_LIMIT);
    }

    /**
     * Sets the size of a grid in tile pixels that will be used to compute a "location hash" of points rendered in each
     * zoom-level for limiting the density of features in the output tile, or computing a "rank" key that clients can
     * use to control label density.
     * <p>
     * If limit is set, features will be dropped automatically before encoding the vector tile, but "rank" must be added
     * explicitly in {@link Profile#postProcessLayerFeatures(String, int, List)}.
     * <p>
     * Replaces any previous values set for label grid pixel size.
     * <p>
     * NOTE: the label grid is computed within each tile independently of its neighbors, so to ensure consistent limits
     * and ranking of a point rendered in adjacent tiles, be sure to set the buffer pixel size to at least be larger
     * than the label grid pixel size at each zoom level.
     *
     * @param labelGridSize a function that returns the size of the label grid to use at each zoom level. If function is
     *                      or returns null for a zoom-level, no label grid will be computed.
     * @see <a href="https://github.com/mapbox/postgis-vt-util/blob/master/src/LabelGrid.sql">LabelGrid postgis
     *      function</a>
     */
    public Feature setPointLabelGridPixelSize(ZoomFunction<Number> labelGridSize) {
      this.labelGridPixelSize = labelGridSize;
      return this;
    }

    /**
     * Sets the size of a grid in tile pixels that will be used to compute a "location hash" of points rendered in each
     * zoom-level at and below {@code maxzoom}.
     * <p>
     * This is a thin wrapper around {@link #setPointLabelGridPixelSize(ZoomFunction)}. It replaces any previous value
     * set for label grid size. To set multiple thresholds, use the other method directly.
     *
     * @see <a href="https://github.com/mapbox/postgis-vt-util/blob/master/src/LabelGrid.sql">LabelGrid postgis
     *      function</a>
     */
    public Feature setPointLabelGridPixelSize(int maxzoom, double size) {
      return setPointLabelGridPixelSize(ZoomFunction.maxZoom(maxzoom, size));
    }

    /**
     * Sets the maximum number of points with the lowest sort-key to include with the same label grid hash in a tile.
     * <p>
     * Replaces any previous values set for label grid limit.
     *
     * @param labelGridLimit a function that returns the size of the label grid to use at each zoom level. If function
     *                       is or returns null for a zoom-level, no label grid will be computed.
     * @see <a href="https://github.com/mapbox/postgis-vt-util/blob/master/src/LabelGrid.sql">LabelGrid postgis
     *      function</a>
     */
    public Feature setPointLabelGridLimit(ZoomFunction<Number> labelGridLimit) {
      this.labelGridLimit = labelGridLimit;
      return this;
    }

    /**
     * Limits the density of points on an output tile at and below {@code maxzoom} by only emitting the {@code limit}
     * features with lowest sort-key in each square of a {@code size x size} pixel grid.
     * <p>
     * This is a thin wrapper around {@link #setPointLabelGridPixelSize(ZoomFunction)} and
     * {@link #setPointLabelGridLimit(ZoomFunction)}. It replaces any previous value set for label grid size or limit.
     * To set multiple thresholds, use the other methods directly.
     * <p>
     * NOTE: the label grid is computed within each tile independently of its neighbors, so to ensure consistent limits
     * and ranking of a point rendered in adjacent tiles, be sure to set the buffer pixel size to at least be larger
     * than the label grid pixel size at each zoom level.
     *
     * @param maxzoom the zoom-level at and below which we should limit point density
     * @param size    the label grid size to use when computing hashes
     * @param limit   the number of lowest-sort-key points to include in each square of the grid
     * @see <a href="https://github.com/mapbox/postgis-vt-util/blob/master/src/LabelGrid.sql">LabelGrid postgis
     *      function</a>
     */
    public Feature setPointLabelGridSizeAndLimit(int maxzoom, double size, int limit) {
      return setPointLabelGridPixelSize(ZoomFunction.maxZoom(maxzoom, size))
        .setPointLabelGridLimit(ZoomFunction.maxZoom(maxzoom, limit));
    }

    // could be expensive, so cache results
    private Map<String, Object> computeAttrsAtZoom(int zoom) {
      Map<String, Object> result = new TreeMap<>();
      for (var entry : attrs.entrySet()) {
        Object value = unwrap(entry.getValue(), zoom);
        if (value != null && !"".equals(value)) {
          result.put(entry.getKey(), value);
        }
      }
      return result;
    }

    private static Object unwrap(Object object, int zoom) {
      for (int i = 0; i < 100; i++) {
        switch (object) {
          case ZoomFunction<?> fn -> object = fn.apply(zoom);
          case Struct struct -> object = struct.rawValue();
          case null, default -> {
            return object;
          }
        }
      }
      throw new IllegalStateException("Failed to unwrap at z" + zoom + ": " + object);
    }

    /** Returns the attribute to put on all output vector tile features at a zoom level. */
    public Map<String, Object> getAttrsAtZoom(int zoom) {
      if (!mustUnwrapValues) {
        return attrs;
      }
      if (attrCache == null) {
        attrCache = CacheByZoom.create(this::computeAttrsAtZoom);
      }
      return attrCache.get(zoom);
    }


    @Override
    public Feature setAttr(String key, Object value) {
      if (value instanceof ZoomFunction || value instanceof Struct) {
        mustUnwrapValues = true;
      }
      if (value != null) {
        attrs.put(key, value);
      }
      return this;
    }

    @Override
    public Feature putAttrs(Map<String, Object> attrs) {
      for (Object value : attrs.values()) {
        if (value instanceof ZoomFunction || value instanceof Struct) {
          mustUnwrapValues = true;
          break;
        }
      }
      this.attrs.putAll(attrs);
      return this;
    }

    @Override
    public FeatureCollector collector() {
      return FeatureCollector.this;
    }

    /**
     * Returns the attribute key that the renderer should use to store the number of points in the simplified geometry
     * before slicing it into tiles.
     */
    public String getNumPointsAttr() {
      return numPointsAttr;
    }

    /**
     * Sets a special attribute key that the renderer will use to store the number of points in the simplified geometry
     * before slicing it into tiles.
     */
    public Feature setNumPointsAttr(String numPointsAttr) {
      this.numPointsAttr = numPointsAttr;
      return this;
    }

    /** Omit this feature from the output */
    public Feature omit() {
      output.remove(this);
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

    /** Returns the actual pixel size of the source feature at {@code zoom} (length if line, sqrt(area) if polygon). */
    public double getSourceFeaturePixelSizeAtZoom(int zoom) {
      return getPixelSizeAtZoom(zoom);
    }

    /**
     * Returns a {@link LinearRange} that can be used to configure attributes that apply to only a portion of this line
     * from {@code start} to {@code end} where 0 is the beginning of the line and 1 is the end.
     * <p>
     * Since mapbox vector tiles can't handle this natively, the line will be broken up into multiple lines in the
     * output tiles at each zoom level with the unique sets of tags on each line. Adjacent segments with the same tags
     * will get merged into a single segment.
     */
    public LinearRange linearRange(double start, double end) {
      return linearRange(Range.closedOpen(start, end));
    }

    /**
     * Returns a {@link LinearRange} that can be used to configure attributes that apply to only a portion of this line
     * from {@code range.lowerBound} to {@code range.lowerBound} where 0 is the beginning of the line and 1 is the end.
     * <p>
     * Since mapbox vector tiles can't handle this natively, the line will be broken up into multiple lines in the
     * output tiles at each zoom level with the unique sets of tags on each line. Adjacent segments with the same tags
     * will get merged into a single segment.
     */
    public LinearRange linearRange(Range<Double> range) {
      return new LinearRange(range);
    }

    /** Returns true if any attributes have been configured over a subset of this line. */
    public boolean hasLinearRanges() {
      return partialOverrides != null;
    }

    /** Computes and returns the linear-scoped attributes of this line, and the geometry they apply to. */
    public List<RangeWithTags> getLinearRangesAtZoom(int zoom) {
      if (partialOverrides == null) {
        return List.of();
      }
      if (partialRangeCache == null) {
        partialRangeCache = CacheByZoom.create(this::computeLinearRangesAtZoom);
      }
      return partialRangeCache.get(zoom);
    }

    private List<RangeWithTags> computeLinearRangesAtZoom(int zoom) {
      record Partial(boolean omit, Map<String, Object> attrs) {
        Partial withOmit(boolean newValue) {
          return new Partial(newValue || omit, attrs);
        }

        Partial merge(Partial other) {
          return new Partial(other.omit, MapUtil.merge(attrs, other.attrs));
        }

        Partial withAttr(String key, Object value) {
          return new Partial(omit, MapUtil.with(attrs, key, value));
        }
      }
      MergingRangeMap<Partial> result = MergingRangeMap.unit(new Partial(false, getAttrsAtZoom(zoom)), Partial::merge);
      for (var override : partialOverrides) {
        result.update(override.range(), m -> switch (override) {
          case Attr attr -> m.withAttr(attr.key, unwrap(attr.value, zoom));
          case Maxzoom mz -> m.withOmit(mz.maxzoom < zoom);
          case Minzoom mz -> m.withOmit(mz.minzoom > zoom);
          case Omit ignored -> m.withOmit(true);
        });
      }
      var ranges = result.result();
      List<RangeWithTags> rangesWithGeometries = new ArrayList<>(ranges.size());
      for (var range : ranges) {
        var value = range.value();
        if (!value.omit) {
          try {
            rangesWithGeometries.add(new RangeWithTags(
              range.start(),
              range.end(),
              source.partialLine(range.start(), range.end()),
              value.attrs
            ));
          } catch (GeometryException e) {
            throw new IllegalStateException(e);
          }
        }
      }
      return rangesWithGeometries;
    }

    public SourceFeature source() {
      return source;
    }


    /**
     * A builder that can be used to configure linear-scoped attributes for a partial segment of a line feature.
     */
    public final class LinearRange implements WithZoomRange<LinearRange>, WithAttrs<LinearRange> {

      private final Range<Double> range;

      private LinearRange(Range<Double> range) {
        this.range = range;
      }

      private LinearRange add(OverrideCommand override) {
        if (partialOverrides == null) {
          partialOverrides = new ArrayList<>();
        }
        partialOverrides.add(override);
        return this;
      }

      @Override
      public LinearRange setMinZoom(int min) {
        return add(new Minzoom(range, min));
      }

      @Override
      public LinearRange setMaxZoom(int max) {
        return add(new Maxzoom(range, max));
      }

      @Override
      public LinearRange setAttr(String key, Object value) {
        if (value instanceof ZoomFunction<?> || value instanceof Struct) {
          mustUnwrapValues = true;
        }
        return add(new Attr(range, key, value));
      }

      /** Exclude this segment of the line feature at all zoom levels. */
      public LinearRange omit() {
        return add(new Omit(range));
      }

      /** Returns the full line {@link Feature} that this segment came from. */
      public Feature entireLine() {
        return Feature.this;
      }

      /**
       * Returns a segment of the full parent line (not the current segment) that can be configured further.
       *
       * @see Feature#linearRange(double, double)
       */
      public LinearRange linearRange(double start, double end) {
        return entireLine().linearRange(start, end);
      }

      /**
       * Returns a segment of the full parent line (not the current segment) that can be configured further.
       *
       * @see Feature#linearRange(Range)
       */
      public LinearRange linearRange(Range<Double> range) {
        return entireLine().linearRange(range);
      }


      @Override
      public int getMinZoomForPixelSize(double minPixelSize) {
        return WithAttrs.super.getMinZoomForPixelSize(minPixelSize / (range.upperEndpoint() - range.lowerEndpoint()));
      }

      @Override
      public FeatureCollector collector() {
        return FeatureCollector.this;
      }
    }
  }
}
