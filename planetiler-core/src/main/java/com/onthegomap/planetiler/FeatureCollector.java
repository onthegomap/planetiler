package com.onthegomap.planetiler;

import com.google.common.collect.Range;
import com.onthegomap.planetiler.collection.FeatureGroup;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.geo.GeometryType;
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

  public Feature geometryNotAddOutPut(String layer, Geometry geometry) {
    // TODO args could also provide a list of source IDs to put into slot 4, 5, 6, etc..
    // to differentiate between other sources besides just OSM and "other"
    long vectorTileId = config.featureSourceIdMultiplier() < 4 ? source.id() :
      source.vectorTileFeatureId(config.featureSourceIdMultiplier());
    return new Feature(layer, geometry, vectorTileId);
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
   * 开始构建从start到end的新的部分线特征，其中0是线的起点，1是线的终点。
   * 如果源特征不能是线，则记录错误并返回一个可以配置但实际上不会向地图发出任何内容的特征。
   *
   * @param layer 该特征将被写入的输出矢量瓦片层
   * @return 可以进一步配置的特征。
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
   * 开始构建一个新多边形地图特征，期望源特征是多边形。
   * 如果源特征不能是多边形，则记录错误并返回一个可以配置但实际上不会向地图发出任何内容的特征。
   * 一些OSM封闭的OSM路径可以是多边形也可以是线
   *
   * @param layer 该特征将被写入的输出矢量瓦片层
   * @return 可以进一步配置的特征。
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
   * 开始构建一个新点、多边形或线地图特征，基于输入特征的几何类型。
   *
   * @param layer 该特征将被写入的输出矢量瓦片层
   * @return 可以进一步配置的特征。
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
   * 开始构建一个新点地图特征，其几何形状来自源特征的 {@link Geometry#getCentroid()}。
   *
   * @param layer 该特征将被写入的输出矢量瓦片层
   * @return 可以进一步配置的特征。
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
   * 开始构建一个新点地图特征，其几何形状来自 {@link Geometry#getCentroid()} 如果源特征是一个点、线或简单凸多边形，
   * 或者 {@link Geometry#getInteriorPoint()} 如果它是多边形、带洞多边形或凹的简单多边形。
   *
   * @param layer 该特征将被写入的输出矢量瓦片层
   * @return 可以进一步配置的特征。
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
   * 开始构建一个新点地图特征，其几何形状来自源特征的 {@link Geometry#getInteriorPoint()}。
   *
   * @param layer 该特征将被写入的输出矢量瓦片层
   * @return 可以进一步配置的特征。
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
   * 开始构建一个新点地图特征，在离多边形边缘最远的内部点使用 {@link MaximumInscribedCircle}（又名“不可达极”）的源特征。
   * 注意：计算比 {@link #centroid(String)} 或 {@link #pointOnSurface(String)} 更加昂贵，尤其是对于较小的 {@code tolerance} 值。
   *
   * @param layer     该特征将被写入的输出矢量瓦片层
   * @param tolerance 用于计算最大内切圆的精度。0.01表示面积平方根的1%。较小的值更精确，但计算成本更高。5%到10%的值是性能和精度的良好折中。
   * @return 可以进一步配置的特征。
   */
  public Feature innermostPoint(String layer, double tolerance) {
    try {
      return geometry(layer, source.innermostPoint(tolerance));
    } catch (GeometryException e) {
      e.log(stats, "feature_innermost_point", "Error constructing innermost point for " + source);
      return empty(layer);
    }
  }

  /** 默认公差为10%的 {@link #innermostPoint(String, double)} 的别名。 */
  public Feature innermostPoint(String layer) {
    return innermostPoint(layer, 0.1);
  }

  /** 返回此特征至少为 {@code pixelSize} 像素大的最小缩放级别。 */
  public int getMinZoomForPixelSize(double pixelSize) {
    try {
      return GeoUtils.minZoomForPixelSize(source.size(), pixelSize);
    } catch (GeometryException e) {
      e.log(stats, "min_zoom_for_size_failure", "Error getting min zoom for size from geometry " + source);
      return config.maxzoom();
    }
  }

  /** 返回源特征在 {@code zoom} 级别的实际像素大小（如果是线，则为长度，如果是多边形，则为面积的平方根）。 */
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
     * 设置此特征出现的缩放范围（包括）。
     * 如果未调用，则默认为所有缩放级别。
     */
    default T setZoomRange(int min, int max) {
      assert min <= max;
      return setMinZoom(min).setMaxZoom(max);
    }


    /**
     * 设置此特征出现的最小缩放级别（包括）。
     * 如果未调用，则默认为地图的最小缩放级别。
     */
    T setMinZoom(int min);

    /**
     * 设置此特征出现的最大缩放级别（包括）。
     * 如果未调用，则默认为地图的最大缩放级别。
     */
    T setMaxZoom(int max);
  }

  public interface WithSelf<T extends WithSelf<T>> {

    default T self() {
      return (T) this;
    }
  }

  public interface WithAttrs<T extends WithAttrs<T>> extends WithSelf<T> {

    /** 从源特征复制 {@code key} 属性的值到输出特征。 */
    default T inheritAttrFromSource(String key) {
      return setAttr(key, collector().source.getTag(key));
    }

    /** 从源特征复制 {@code keys} 属性的值到输出特征。 */
    default T inheritAttrsFromSource(String... keys) {
      for (var key : keys) {
        inheritAttrFromSource(key);
      }
      return self();
    }

    /** 从源特征复制 {@code keys} 属性的值到输出特征。 */
    default T inheritAttrsFromSourceWithMinzoom(int minzoom, String... keys) {
      for (var key : keys) {
        setAttrWithMinzoom(key, collector().source.getTag(key), minzoom);
      }
      return self();
    }

    /**
     * 设置输出特征的属性为字符串、数字、布尔值或 {@link ZoomFunction} 实例，
     * 以按缩放级别更改 {@code key} 的值。
     */
    T setAttr(String key, Object value);

    /**
     * 设置 {@code key} 属性在或以上 {@code minzoom} 的值。低于 {@code minzoom} 则忽略。
     * 替换所有先前在任何缩放级别为 {@code key} 设置的值。要在多个缩放级别阈值更改值，请调用
     * {@link #setAttr(String, Object)} 并手动构建 {@link ZoomFunction} 值。
     */
    default T setAttrWithMinzoom(String key, Object value, int minzoom) {
      return setAttr(key, ZoomFunction.minZoom(minzoom, value));
    }

    /**
     * 设置 {@code key} 属性的值仅在特征至少为 {@code minPixelSize} 像素大小的缩放级别。
     */
    default T setAttrWithMinSize(String key, Object value, double minPixelSize) {
      return setAttrWithMinzoom(key, value, getMinZoomForPixelSize(minPixelSize));
    }

    /**
     * 设置 {@code key} 属性的值，使其始终显示在 {@code zoom_level >= minZoomToShowAlways}，
     * 但仅在 {@code minZoomIfBigEnough <= zoom_level < minZoomToShowAlways} 时显示，
     * 如果其至少为 {@code minPixelSize} 像素大小。
     * 如果需要更大的灵活性，请直接使用 {@link #getMinZoomForPixelSize(double)}，
     * 或创建计算 {@link #getPixelSizeAtZoom(int)} 并应用自定义阈值的 {@link ZoomFunction}。
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
     * 在或以上 {@code minzoom} 将所有 {@code attrs} 中的键/值对插入到输出特征的属性集中。
     * 替换已经设置的值。
     */
    default T putAttrsWithMinzoom(Map<String, Object> attrs, int minzoom) {
      for (var entry : attrs.entrySet()) {
        setAttrWithMinzoom(entry.getKey(), entry.getValue(), minzoom);
      }
      return self();
    }

    /**
     * 将所有 {@code attrs} 中的键/值对插入到输出特征的属性集中。
     * 不触碰已经设置的属性。
     * {@code attrs} 中的值可以是要设置的原始值，或 {@link ZoomFunction} 实例，
     * 以按缩放级别更改该属性的值。
     */
    default T putAttrs(Map<String, Object> attrs) {
      for (var entry : attrs.entrySet()) {
        setAttr(entry.getKey(), entry.getValue());
      }
      return self();
    }

    /** 返回此特征来自的 {@link FeatureCollector}。 */
    FeatureCollector collector();
  }

  /**
   * 为我们遇到的每个源特征创建新的特征收集器实例。
   */
  public record Factory(PlanetilerConfig config, Stats stats) {

    public FeatureCollector get(SourceFeature source) {
      return new FeatureCollector(source, config, stats);
    }
  }

  private record PartialOverride(Range<Double> range, Object key, Object value) {}

  /** 一个完全配置的线特征子集，具有应用于范围子集的线性范围属性。 */
  public record RangeWithTags(double start, double end, Geometry geom, Map<String, Object> attrs) {}

  /**
   * 一个用于构建输出地图特征的构建器，包含从输入要素中渲染矢量瓦片特征所需的所有信息。
   * 一些特征属性是全局设置的（如排序键），一些允许按缩放级别更改值（如标签）。
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

    /** 返回此特征来源的源特征的原始ID（即OSM节点/路径ID）。 */
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
     * 返回特征在输出矢量瓦片中的层内排序的值。
     */
    public int getSortKey() {
      return sortKey;
    }

    /**
     * 设置特征在输出矢量瓦片中的层内排序的值。排序键被打包到 {@link FeatureGroup#SORT_KEY_BITS} 位中，
     * 因此此范围的值限制在 {@code -(2^(bits-1))} 到 {@code (2^(bits-1))-1}。
     * 圆形、线条和多边形按它们在每层中出现的顺序呈现，因此在每层中较晚出现（较高排序键）的特征显示在较低排序键特征之上。
     * 对于尝试避免标签碰撞的符号（文本/图标），特征按每层中出现的顺序放置，因此在每层中较早出现（较低排序键）的特征
     * 将显示在比较晚出现（较高排序键）的特征的较低缩放级别。
     */
    public Feature setSortKey(int sortKey) {
      assert sortKey >= FeatureGroup.SORT_KEY_MIN && sortKey <= FeatureGroup.SORT_KEY_MAX : "Sort key " + sortKey +
        " outside of allowed range [" + FeatureGroup.SORT_KEY_MIN + ", " + FeatureGroup.SORT_KEY_MAX + "]";
      this.sortKey = sortKey;
      return this;
    }

    /** 设置特征在输出矢量瓦片中的层内从高到低排序的值。 */
    public Feature setSortKeyDescending(int sortKey) {
      return setSortKey(FeatureGroup.SORT_KEY_MAX + FeatureGroup.SORT_KEY_MIN - sortKey);
    }

    /** 返回此特征出现的最小缩放级别（包括）。 */
    public int getMinZoom() {
      return minzoom;
    }

    @Override
    public Feature setMinZoom(int min) {
      minzoom = Math.max(min, config.minzoom());
      return this;
    }

    /** 返回此特征出现的最大缩放级别（包括）。 */
    public int getMaxZoom() {
      return maxzoom;
    }

    @Override
    public Feature setMaxZoom(int max) {
      maxzoom = Math.min(max, config.maxzoom());
      return this;
    }

    /** 返回此特征将出现在的输出矢量瓦片层。 */
    public String getLayer() {
      return layer;
    }

    /**
     * 返回此特征的JTS几何形状（在世界Web墨卡托坐标中）。
     * 后续在 {@link FeatureRenderer} 中的后处理将把它切成瓦片几何。
     */
    public Geometry getGeometry() {
      return geom;
    }

    /** 返回在 {@code zoom} 级别渲染的瓦片边界外的细节像素数量。 */
    public double getBufferPixelsAtZoom(int zoom) {
      return ZoomFunction.applyAsDoubleOrElse(bufferPixelOverrides, zoom, defaultBufferPixels);
    }

    /**
     * 设置在未设置缩放级别特定覆盖时，渲染的瓦片边界外的细节像素的默认数量 {@link #setBufferPixelOverrides(ZoomFunction)}。
     */
    public Feature setBufferPixels(double buffer) {
      defaultBufferPixels = buffer;
      return this;
    }

    /**
     * 设置在缩放级别特定覆盖的瓦片边界外的细节像素数量。
     * 如果 {@code buffer} 为 {@code null} 或返回 {@code null}，则缓冲像素将默认为 {@link #setBufferPixels(double)}。
     */
    public Feature setBufferPixelOverrides(ZoomFunction<Number> buffer) {
      bufferPixelOverrides = buffer;
      return this;
    }

    /**
     * 返回特征在 {@code zoom} 级别的最小分辨率（以像素为单位）。
     * 对于线特征，这是长度，对于多边形特征，这是要发出的最小面积的平方根。
     */
    public double getMinPixelSizeAtZoom(int zoom) {
      return zoom == config.maxzoomForRendering() ? minPixelSizeAtMaxZoom :
        ZoomFunction.applyAsDoubleOrElse(minPixelSize, zoom, defaultMinPixelSize);
    }

    /**
     * 设置在地图最大缩放级别以下发出的线特征最小长度或多边形特征的最小面积的平方根。
     * 在地图的最大缩放级别，客户端可以“过度缩放”到特征上，因此这将在最大缩放级别上将最小尺寸保持为
     * {@link PlanetilerConfig#minFeatureSizeAtMaxZoom()}，除非明确覆盖它 {@link #setMinPixelSizeAtMaxZoom(double)}
     * 或 {@link #setMinPixelSizeAtAllZooms(int)}。
     */
    public Feature setMinPixelSize(double minPixelSize) {
      this.defaultMinPixelSize = minPixelSize;
      return this;
    }

    /**
     * 设置在地图最大缩放级别以下发出的线特征最小长度或多边形特征的最小面积的平方根。
     * 在地图的最大缩放级别，客户端可以“过度缩放”到特征上，因此这将在最大缩放级别上将最小尺寸保持为
     * {@link PlanetilerConfig#minFeatureSizeAtMaxZoom()}，除非明确覆盖它 {@link #setMinPixelSizeAtMaxZoom(double)}
     * 或 {@link #setMinPixelSizeAtAllZooms(int)}。
     * 如果 {@code levels} 为 {@code null} 或返回 {@code null}，则最小像素尺寸将默认为默认值。
     */
    public Feature setMinPixelSizeOverrides(ZoomFunction<Number> levels) {
      this.minPixelSize = levels;
      return this;
    }

    /**
     * 使用 {@code minPixelSize} 覆盖默认的最小像素尺寸，并在或以下 {@code zoom} 发出特征。
     * 这将替换所有先前设置的缩放覆盖。要使用多个缩放级别阈值，请显式创建一个 {@link ZoomFunction} 并将其传递给 {@link #setMinPixelSizeOverrides(ZoomFunction)}。
     */
    public Feature setMinPixelSizeBelowZoom(int zoom, double minPixelSize) {
      if (zoom >= config.maxzoomForRendering()) {
        minPixelSizeAtMaxZoom = minPixelSize;
      }
      this.minPixelSize = ZoomFunction.maxZoom(zoom, minPixelSize);
      return this;
    }

    /**
     * 设置在地图最大缩放级别发出的线特征最小长度或多边形特征的最小面积的平方根。
     * 由于客户端可以“过度缩放”到最大缩放级别上的特征，因此这通常比较低缩放级别上的最小尺寸小得多。
     * 这将覆盖，但不会替换默认的最小像素尺寸或通过其他方法设置的覆盖。
     */
    public Feature setMinPixelSizeAtMaxZoom(double minPixelSize) {
      this.minPixelSizeAtMaxZoom = minPixelSize;
      return this;
    }

    /**
     * 在所有缩放级别（包括地图最大缩放级别）设置线特征的最小长度或多边形特征的最小面积的平方根。
     * 这将替换以前的默认值，但不会覆盖通过 {@link #setMinPixelSizeOverrides(ZoomFunction)} 设置的覆盖。
     */
    public Feature setMinPixelSizeAtAllZooms(int minPixelSize) {
      this.minPixelSizeAtMaxZoom = minPixelSize;
      return this.setMinPixelSize(minPixelSize);
    }

    /**
     * 返回在 {@code zoom} 级别的线和多边形的简化容差（以像素为单位）。
     */
    public double getPixelToleranceAtZoom(int zoom) {
      return zoom == config.maxzoomForRendering() ? pixelToleranceAtMaxZoom :
        ZoomFunction.applyAsDoubleOrElse(pixelTolerance, zoom, defaultPixelTolerance);
    }

    /**
     * 设置地图最大缩放级别以下的线和多边形的简化容差（以像素为单位）。
     * 由于客户端可以“过度缩放”到地图的最大缩放级别，这通常比默认容差小，以提供更多细节。
     * 这不会替换通过 {@link #setPixelToleranceOverrides(ZoomFunction)} 设置的覆盖。
     */
    public Feature setPixelTolerance(double tolerance) {
      this.defaultPixelTolerance = tolerance;
      return this;
    }

    /**
     * 设置地图最大缩放级别的线和多边形的简化容差（以像素为单位）。
     * 这不会替换通过 {@link #setPixelTolerance(double)} 设置的默认值或通过 {@link #setPixelToleranceOverrides(ZoomFunction)} 设置的覆盖。
     */
    public Feature setPixelToleranceAtMaxZoom(double tolerance) {
      this.pixelToleranceAtMaxZoom = tolerance;
      return this;
    }

    /**
     * 设置线和多边形的简化容差（以像素为单位），包括地图最大缩放级别。
     * 这不会替换通过 {@link #setPixelTolerance(double)} 设置的默认值。
     */
    public Feature setPixelToleranceAtAllZooms(double tolerance) {
      return setPixelToleranceAtMaxZoom(tolerance).setPixelTolerance(tolerance);
    }

    /**
     * 设置在最大缩放级别以下的地图瓦片中线条和多边形的简化容差的缩放特定覆盖。
     * <p>
     * 在地图的最大缩放级别，客户端可以“过度放大”特征，因此在最大缩放级别将容差设置为 {@link PlanetilerConfig#simplifyToleranceAtMaxZoom()}，
     * 除非你明确通过 {@link #setMinPixelSizeAtAllZooms(int)} 或 {@link #setMinPixelSizeAtMaxZoom(double)} 覆盖它。
     * <p>
     * 如果 {@code levels} 是 {@code null} 或返回 {@code null}，最小像素大小将默认为默认值。
     */
    public Feature setPixelToleranceOverrides(ZoomFunction<Number> overrides) {
      this.pixelTolerance = overrides;
      return this;
    }

    /**
     * 覆盖在 {@code zoom} 及以下缩放级别中瓦片像素中线条和多边形的默认简化容差，使用 {@code minPixelSize}。
     * <p>
     * 这将替换所有先前设置的缩放覆盖。要使用多个缩放级别阈值，请显式创建 {@link ZoomFunction} 并将其传递给 {@link #setPixelToleranceOverrides(ZoomFunction)}。
     */
    public Feature setPixelToleranceBelowZoom(int zoom, double tolerance) {
      if (zoom == config.maxzoomForRendering()) {
        pixelToleranceAtMaxZoom = tolerance;
      }
      return setPixelToleranceOverrides(ZoomFunction.maxZoom(zoom, tolerance));
    }

    /**
     * 判断是否具有标签网格设置。
     */
    public boolean hasLabelGrid() {
      return labelGridPixelSize != null || labelGridLimit != null;
    }

    /**
     * 返回用于分组或限制输出点的网格的像素大小。
     *
     * @throws AssertionError 当断言启用且返回值小于缓冲区像素大小时抛出
     */
    public double getPointLabelGridPixelSizeAtZoom(int zoom) {
      double result = ZoomFunction.applyAsDoubleOrElse(labelGridPixelSize, zoom, DEFAULT_LABEL_GRID_SIZE);
      // TODO 这是否足够？如果网格方块在瓦片开始之前结束怎么办
      assert result <= getBufferPixelsAtZoom(
        zoom) : "to avoid inconsistent rendering of the same point between adjacent tiles, buffer pixel size should be >= label grid size but in '%s' buffer pixel size=%f was greater than label grid size=%f"
          .formatted(
            getLayer(), getBufferPixelsAtZoom(zoom), result);
      return result;
    }

    /**
     * 返回每个网格方块中输出矢量瓦片中包含的最低排序键点的最大数量，网格大小为 {@link #getPointLabelGridPixelSizeAtZoom(int)}。
     */
    public int getPointLabelGridLimitAtZoom(int zoom) {
      return ZoomFunction.applyAsIntOrElse(labelGridLimit, zoom, DEFAULT_LABEL_GRID_LIMIT);
    }

    /**
     * 设置在每个缩放级别计算标签网格哈希值时用于分组或限制输出点的网格大小（以像素为单位），
     * 或者计算客户端可以用来控制标签密度的“排序”键。
     * 如果设置了限制，则特征将在编码矢量瓦片之前自动删除，但必须在 {@link Profile#postProcessLayerFeatures(String, int, List)} 中显式添加“排序”。
     * 替换之前为标签网格像素大小设置的任何值。
     * 注意：标签网格在每个瓦片内独立计算，因此为了确保一致的限制和标签排序，请确保在每个缩放级别将缓冲像素大小设置为至少大于标签网格像素大小。
     *
     * @param labelGridSize 在每个缩放级别使用的标签网格大小。如果函数为空或在某个缩放级别返回空，则不会计算标签网格。
     * @see <a href="https://github.com/mapbox/postgis-vt-util/blob/master/src/LabelGrid.sql">LabelGrid postgis 函数</a>
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
     * 设置在每个缩放级别计算标签网格哈希值时用于限制输出点密度的点数上限（以像素为单位）。
     * 替换之前为标签网格限制设置的任何值。
     *
     * @param labelGridLimit 在每个缩放级别使用的标签网格限制。如果函数为空或在某个缩放级别返回空，则不会计算标签网格。
     * @see <a href="https://github.com/mapbox/postgis-vt-util/blob/master/src/LabelGrid.sql">LabelGrid postgis 函数</a>
     */
    public Feature setPointLabelGridLimit(ZoomFunction<Number> labelGridLimit) {
      this.labelGridLimit = labelGridLimit;
      return this;
    }

    /**
     * 在或以下 {@code maxzoom} 限制输出瓦片上的点密度，仅发出具有最低排序键的 {@code limit} 特征在 {@code size x size} 像素网格的每个方格中。
     * 这是 {@link #setPointLabelGridPixelSize(ZoomFunction)} 和 {@link #setPointLabelGridLimit(ZoomFunction)} 的薄包装。
     * 它会替换以前为标签网格大小或限制设置的任何值。要设置多个阈值，请直接使用其他方法。
     * 注意：标签网格在每个瓦片内独立计算，因此为了确保一致的限制和标签排序，请确保在每个缩放级别将缓冲像素大小设置为至少大于标签网格像素大小。
     *
     * @param maxzoom 在该缩放级别或以下限制点密度
     * @param size    计算哈希时使用的标签网格大小
     * @param limit   在网格的每个方格中包含的最低排序键点的数量
     * @see <a href="https://github.com/mapbox/postgis-vt-util/blob/master/src/LabelGrid.sql">LabelGrid postgis 函数</a>
     */
    public Feature setPointLabelGridSizeAndLimit(int maxzoom, double size, int limit) {
      return setPointLabelGridPixelSize(ZoomFunction.maxZoom(maxzoom, size))
        .setPointLabelGridLimit(ZoomFunction.maxZoom(maxzoom, limit));
    }

    // 可能昂贵，所以缓存结果
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

    /** 返回在缩放级别上输出矢量瓦片特征上的属性。 */
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
     * 返回渲染器在将简化几何切片到瓦片中之前应使用的点数的特殊属性键。
     */
    public String getNumPointsAttr() {
      return numPointsAttr;
    }

    /**
     * 设置渲染器将在将简化几何切片到瓦片中之前使用的点数的特殊属性键。
     */
    public Feature setNumPointsAttr(String numPointsAttr) {
      this.numPointsAttr = numPointsAttr;
      return this;
    }

    /** 从输出中省略此特征 */
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

    /** 返回源特征在 {@code zoom} 级别的实际像素大小（如果是线，则为长度，如果是多边形，则为面积的平方根）。 */
    public double getSourceFeaturePixelSizeAtZoom(int zoom) {
      return getPixelSizeAtZoom(zoom);
    }

    /**
     * 返回一个 {@link LinearRange}，可用于配置仅应用于此线的一部分从 {@code start} 到 {@code end} 的属性，其中0是线的起点，1是线的终点。
     * 由于mapbox矢量瓦片本身不能处理此功能，因此在每个缩放级别的输出瓦片中将线分成多条线，并在每条线上设置唯一的标签集。具有相同标签的相邻段将合并为一个段。
     */
    public LinearRange linearRange(double start, double end) {
      return linearRange(Range.closedOpen(start, end));
    }

    /**
     * 返回一个 {@link LinearRange}，可用于配置仅应用于此线的一部分从 {@code range.lowerBound} 到 {@code range.lowerBound} 的属性，其中0是线的起点，1是线的终点。
     * 由于mapbox矢量瓦片本身不能处理此功能，因此在每个缩放级别的输出瓦片中将线分成多条线，并在每条线上设置唯一的标签集。具有相同标签的相邻段将合并为一个段。
     */
    public LinearRange linearRange(Range<Double> range) {
      return new LinearRange(range);
    }

    /** 返回是否为此线的子集配置了任何属性。 */
    public boolean hasLinearRanges() {
      return partialOverrides != null;
    }

    /** 计算并返回此线的线性范围属性及其应用的几何形状。 */
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

    /**
     * 用于配置线性范围属性的构建器，适用于线特征的一部分。
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

      /** 在所有缩放级别上排除此线特征的这一段。 */
      public LinearRange omit() {
        return add(new Omit(range));
      }

      /** 返回该段的完整线 {@link Feature}。 */
      public Feature entireLine() {
        return Feature.this;
      }

      /**
       * 返回可以进一步配置的完整父线段（不是当前段）。
       * 参见 Feature#linearRange(double, double)
       */
      public LinearRange linearRange(double start, double end) {
        return entireLine().linearRange(start, end);
      }

      /**
       * 返回可以进一步配置的完整父线段（不是当前段）。
       * 参见 Feature#linearRange(Range)
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
