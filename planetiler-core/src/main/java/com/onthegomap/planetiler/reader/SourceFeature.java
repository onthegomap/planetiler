package com.onthegomap.planetiler.reader;

import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.geo.LineSplitter;
import com.onthegomap.planetiler.reader.osm.OsmReader;
import com.onthegomap.planetiler.reader.osm.OsmRelationInfo;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.locationtech.jts.algorithm.construct.MaximumInscribedCircle;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Lineal;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;

/**
 * 从数据源读取的输入特征的基类。
 * <p>
 * 提供了带有惰性初始化的几何属性的缓存便利方法，这些方法是从 {@link #latLonGeometry()} 和 {@link #worldGeometry()} 派生的，
 * 以避免在不需要时进行计算，并在多个特征需要时重新计算。
 * <p>
 * 除 {@link #latLonGeometry()} 之外的所有几何体都返回以世界 Web 墨卡托坐标表示的元素，其中 (0,0) 是西北角，(1,1) 是地球的东南角。
 */
public abstract class SourceFeature implements WithTags, WithGeometryType {

  private final Map<String, Object> tags;
  private final String source;
  private final String sourceLayer;
  private final List<OsmReader.RelationMember<OsmRelationInfo>> relationInfos;
  private final long id;
  private Geometry centroid = null;
  private Geometry pointOnSurface = null;
  private Geometry centroidIfConvex = null;
  private double innermostPointTolerance = Double.NaN;
  private Geometry innermostPoint = null;
  private Geometry linearGeometry = null;
  private Geometry polygonGeometry = null;
  private Geometry validPolygon = null;
  private double area = Double.NaN;
  private double length = Double.NaN;
  private double size = Double.NaN;
  private LineSplitter lineSplitter;

  /**
   * 构造一个新的输入特征。
   *
   * @param tags          与该元素关联的字符串键/值对
   * @param source        配置文件可用于区分来自不同数据源的元素的源名称
   * @param sourceLayer   配置文件可用于区分给定源中不同类型元素的图层名称
   * @param relationInfos 包含该元素的关系
   * @param id            该特征在该源中的数字 ID（即 OSM 元素 ID）
   */
  protected SourceFeature(Map<String, Object> tags, String source, String sourceLayer,
    List<OsmReader.RelationMember<OsmRelationInfo>> relationInfos, long id) {
    this.tags = tags;
    this.source = source;
    this.sourceLayer = sourceLayer;
    this.relationInfos = relationInfos;
    this.id = id;
  }


  @Override
  public Map<String, Object> tags() {
    return tags;
  }

  /**
   * 返回此特征在纬度/经度坐标中的几何体。
   *
   * @return 纬度/经度几何体
   * @throws GeometryException         如果创建此几何体时发生意外但可恢复的错误，应记录以进行调试
   * @throws GeometryException.Verbose 如果创建此几何体时发生预期错误，将在较低日志级别记录
   */
  public abstract Geometry latLonGeometry() throws GeometryException;

  /**
   * 返回此特征在世界 Web 墨卡托坐标中的几何体。
   *
   * @return 墨卡托坐标中的几何体
   * @throws GeometryException         如果创建此几何体时发生意外但可恢复的错误，应记录以进行调试
   * @throws GeometryException.Verbose 如果创建此几何体时发生预期错误，将在较低日志级别记录
   */
  public abstract Geometry worldGeometry() throws GeometryException;

  /** 返回并缓存此几何体在世界 Web 墨卡托坐标中的 {@link Geometry#getCentroid()}。 */
  public final Geometry centroid() throws GeometryException {
    return centroid != null ? centroid : (centroid =
      canBePolygon() ? polygon().getCentroid() :
        canBeLine() ? line().getCentroid() :
        worldGeometry().getCentroid());
  }

  /** 返回并缓存此几何体在世界 Web 墨卡托坐标中的 {@link Geometry#getInteriorPoint()}。 */
  public final Geometry pointOnSurface() throws GeometryException {
    return pointOnSurface != null ? pointOnSurface : (pointOnSurface =
      canBePolygon() ? polygon().getInteriorPoint() :
        canBeLine() ? line().getInteriorPoint() :
        worldGeometry().getInteriorPoint());
  }

  /**
   * 返回此几何体在世界 Web 墨卡托坐标中的 {@link MaximumInscribedCircle#getCenter()}。
   *
   * @param tolerance 计算最大内切圆的精度。0.01 表示面积平方根的 1%。较小的值精度更高，但计算代价昂贵。0.05-0.1 是性能与精度的良好折衷。
   */
  public final Geometry innermostPoint(double tolerance) throws GeometryException {
    if (canBePolygon()) {
      // 缓存，只要容差没有改变
      if (tolerance != innermostPointTolerance || innermostPoint == null) {
        innermostPoint = MaximumInscribedCircle.getCenter(polygon(), Math.sqrt(area()) * tolerance);
        innermostPointTolerance = tolerance;
      }
      return innermostPoint;
    } else {
      return pointOnSurface();
    }
  }

  private Geometry computeCentroidIfConvex() throws GeometryException {
    if (!canBePolygon()) {
      return centroid();
    } else if (polygon() instanceof Polygon poly &&
      poly.getNumInteriorRing() == 0 &&
      GeoUtils.isConvex(poly.getExteriorRing())) {
      return centroid();
    } else { // multipolygon, polygon with holes, or concave polygon
      return pointOnSurface();
    }
  }

  /**
   * 返回并缓存几何体在世界 Web 墨卡托坐标中的一个点。
   * <p>
   * 如果几何体是凸的，使用更快的 {@link Geometry#getCentroid()}，否则使用较慢的 {@link Geometry#getInteriorPoint()}。
   */
  public final Geometry centroidIfConvex() throws GeometryException {
    return centroidIfConvex != null ? centroidIfConvex : (centroidIfConvex = computeCentroidIfConvex());
  }

  /**
   * 计算此特征在世界 Web 墨卡托坐标中的 {@link LineString} 或 {@link MultiLineString}。
   *
   * @return 墨卡托坐标中的线串
   * @throws GeometryException         如果创建此几何体时发生意外但可恢复的错误，应记录以进行调试
   * @throws GeometryException.Verbose 如果创建此几何体时发生预期错误，将在较低日志级别记录
   */
  protected Geometry computeLine() throws GeometryException {
    Geometry world = worldGeometry();
    return world instanceof Lineal ? world : GeoUtils.polygonToLineString(world);
  }

  /**
   * 返回此特征在世界 Web 墨卡托坐标中的 {@link LineString} 或 {@link MultiLineString}。
   *
   * @throws GeometryException 如果在构建几何体时发生错误，或此特征不应解释为线
   */
  public final Geometry line() throws GeometryException {
    if (!canBeLine()) {
      throw new GeometryException("feature_not_line", "cannot be line", true);
    }
    if (linearGeometry == null) {
      linearGeometry = computeLine();
    }
    return linearGeometry;
  }

  /**
   * 返回从 {@code start} 到 {@code end} 的部分线串，其中 0 是线的起点，1 是线的终点。
   *
   * @throws GeometryException 如果在构建几何体时发生错误，或此特征不应解释为单线（不允许多线串）。
   */
  public final Geometry partialLine(double start, double end) throws GeometryException {
    Geometry line = line();
    if (start <= 0 && end >= 1) {
      return line;
    } else if (line instanceof LineString lineString) {
      if (this.lineSplitter == null) {
        this.lineSplitter = new LineSplitter(lineString);
      }
      return lineSplitter.get(start, end);
    } else {
      throw new GeometryException("partial_multilinestring", "cannot get partial of a multiline", true);
    }
  }

  /**
   * 计算此特征在世界 Web 墨卡托坐标中的 {@link Polygon} 或 {@link MultiPolygon}。
   *
   * @return 墨卡托坐标中的多边形
   * @throws GeometryException         如果创建此几何体时发生意外但可恢复的错误，应记录以进行调试
   * @throws GeometryException.Verbose 如果创建此几何体时发生预期错误，将在较低日志级别记录
   */
  protected Geometry computePolygon() throws GeometryException {
    return worldGeometry();
  }

  /**
   * 返回此特征在世界 Web 墨卡托坐标中的 {@link Polygon} 或 {@link MultiPolygon}。
   *
   * @throws GeometryException 如果在构建几何体时发生错误，或此特征不应解释为线
   */
  public final Geometry polygon() throws GeometryException {
    if (!canBePolygon()) {
      throw new GeometryException("feature_not_polygon", "cannot be polygon", true);
    }
    return polygonGeometry != null ? polygonGeometry : (polygonGeometry = computePolygon());
  }

  private Geometry computeValidPolygon() throws GeometryException {
    Geometry polygon = polygon();
    if (!polygon.isValid()) {
      polygon = GeoUtils.fixPolygon(polygon);
    }
    return polygon;
  }

  /**
   * 返回此特征在世界 Web 墨卡托坐标中的有效 {@link Polygon} 或 {@link MultiPolygon}。
   * <p>
   * 验证和修复无效多边形可能代价昂贵，因此仅在必要时使用。无效多边形也将在渲染时修复。
   *
   * @throws GeometryException 如果在构建几何体时发生错误，或此特征不应解释为线
   */
  public final Geometry validatedPolygon() throws GeometryException {
    if (!canBePolygon()) {
      throw new GeometryException("feature_not_polygon", "cannot be polygon", true);
    }
    return validPolygon != null ? validPolygon : (validPolygon = computeValidPolygon());
  }

  /**
   * 返回并缓存此特征在世界 Web 墨卡托坐标中 {@link Geometry#getArea()} 的结果，其中 {@code 1} 表示整个地球的面积。
   */
  public double area() throws GeometryException {
    return Double.isNaN(area) ? (area = canBePolygon() ? polygon().getArea() : 0) : area;
  }

  /**
   * 返回并缓存此特征在世界 Web 墨卡托坐标中 {@link Geometry#getLength()} 的结果，其中 {@code 1} 表示整个地球的周长或从北纬 85 度到南纬 85 度的距离。
   */
  public double length() throws GeometryException {
    return Double.isNaN(length) ? (length =
      (isPoint() || canBePolygon() || canBeLine()) ? worldGeometry().getLength() : 0) : length;
  }

  /**
   * 返回并缓存此特征的面积的平方根（如果是多边形）或长度（如果是线串）。
   */
  public double size() throws GeometryException {
    return Double.isNaN(size) ? (size = canBePolygon() ? Math.sqrt(Math.abs(area())) : canBeLine() ? length() : 0) :
      size;
  }

  /** Returns the ID of the source that this feature came from. */
  public String getSource() {
    return source;
  }

  /** Returns the layer ID within a source that this feature comes from. */
  public String getSourceLayer() {
    return sourceLayer;
  }

  /**
   * 返回包含该元素的 OSM 关系的列表。
   *
   * @param relationInfoClass 处理关系数据的类
   * @param <T>               {@code relationInfoClass} 的类型
   * @return 包含 OSM 关系信息及其在该关系中的角色的列表
   */
  // TODO 这应该在一个专门的 OSM 子类中，而不是通用的超类中
  public <T extends OsmRelationInfo> List<OsmReader.RelationMember<T>> relationInfo(
    Class<T> relationInfoClass) {
    List<OsmReader.RelationMember<T>> result = null;
    if (relationInfos != null) {
      for (OsmReader.RelationMember<?> info : relationInfos) {
        if (relationInfoClass.isInstance(info.relation())) {
          if (result == null) {
            result = new ArrayList<>();
          }
          @SuppressWarnings("unchecked") OsmReader.RelationMember<T> casted = (OsmReader.RelationMember<T>) info;
          result.add(casted);
        }
      }
    }
    return result == null ? List.of() : result;
  }

  /** 返回此元素从输入数据源（即 OSM 元素 ID）获取的 ID。 */
  public final long id() {
    return id;
  }

  /** 默认情况下，特征 ID 是从输入数据源 ID 直接获取的。 */
  public long vectorTileFeatureId(int multiplier) {
    return multiplier * id;
  }

  /** 如果此元素有任何 OSM 关系信息，则返回 true。 */
  public boolean hasRelationInfo() {
    return relationInfos != null && !relationInfos.isEmpty();
  }

  @Override
  public String toString() {
    return "Feature[source=" + getSource() +
      ", source layer=" + getSourceLayer() +
      ", id=" + id() +
      ", tags=" + tags + ']';
  }

}
