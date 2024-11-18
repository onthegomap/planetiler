package com.onthegomap.planetiler.layers;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.GeometryType;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.util.ZoomFunction;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Poi implements Profile {

  private static final Logger LOGGER = LoggerFactory.getLogger(Poi.class);

  public static final String LINESPACE_POI = "LINESPACE_POI";

  /**
   * 默认瓦片最大点数量 256 * 256 * 2
   */
  public static final Long POI_MAX_NUMS = 131072L;

  /**
   * 默认网格集大小
   */
  private static final int EXTENT = 256;

  /**
   * 默认像素大小
   */
  private static final int PIXEL_SIZE = 1;

  /**
   * TODO 该计算方式，瓦片点最大数量可能不完全等于POI_MAX_NUMS
   */
  private static final int GRID_SIZE = (int) Math.ceil((double) POI_MAX_NUMS / (EXTENT * EXTENT));

  private final PoiTilingConfig poiTileConfig;
  private final PlanetilerConfig config;

  private Map<String, HashSet<String>> geomTypes = new HashMap<>();

  public Poi(PlanetilerConfig config, PoiTilingConfig poiTileConfig) {
    this.poiTileConfig = poiTileConfig;
    this.config = config;
  }

  @Override
  public void processFeature(SourceFeature source, FeatureCollector features) {
    String layerName = StringUtils.isBlank(config.layerName()) ? LINESPACE_POI : config.layerName();
    FeatureCollector.Feature feature = features.anyGeometry(layerName);
    configureFeature(feature);

    var tags = source.tags();
    tags.forEach(feature::setAttr);

    Integer minZoom = getFieldValueMinZoom(tags, poiTileConfig.zoomLevelsMap());
    if (minZoom != null) {
      feature.setMinZoom(minZoom);
    }

    // 计算优先级：根据 numericField 或 priorityLevelsMap
    Integer rankOrder = calculateRankOrder(tags);
    if (rankOrder != null) {
      feature.setSortKey(rankOrder);
    }

    String geometryType = null;
    if (source.isPoint()) {
      geometryType = GeometryType.POINT.name();
    } else if (source.canBePolygon()) {
      geometryType = GeometryType.POLYGON.name();
    } else if (source.canBeLine()) {
      geometryType = GeometryType.LINE.name();
    }

    if (StringUtils.isNotBlank(geometryType)) {
      geomTypes.computeIfAbsent(feature.getLayer(), k -> new HashSet<>()).add(geometryType);
    }
  }

  @Override
  public String name() {
    return StringUtils.isBlank(config.layerName()) ? LINESPACE_POI : config.layerName();
  }

  @Override
  public Map<String, HashSet<String>> geomTypes() {
    return geomTypes;
  }

  private void configureFeature(FeatureCollector.Feature feature) {
    if (config.labelGridPixelSize() != null && config.labelGridLimit() != null) {
      feature.setPointLabelGridPixelSize(config.labelGridPixelSize())
        .setPointLabelGridLimit(config.labelGridLimit());

      if (config.bufferPixelOverrides() != null) {
        feature.setBufferPixelOverrides(config.bufferPixelOverrides());
      } else {
        feature.setBufferPixelOverrides(config.labelGridPixelSize());
      }
    } else {
      // 根据系统默认瓦片最大点数量限制瓦片大小,最高层级 14需要展示所有数据
      ZoomFunction<Number> gridPixelSize = ZoomFunction.fromMaxZoomThresholds(Map.of(), PIXEL_SIZE);
      ZoomFunction<Number> gridLimit = ZoomFunction.fromMaxZoomThresholds(Map.of(13, GRID_SIZE));
      feature.setPointLabelGridPixelSize(gridPixelSize)
        .setPointLabelGridLimit(gridLimit)
        .setBufferPixelOverrides(gridPixelSize);
    }
  }

  /**
   * 计算 rankOrder，根据 numericField 或 priorityLevelsMap，
   * numericField优先级高于priorityLevelsMap
   */
  private Integer calculateRankOrder(Map<String, Object> tags) {
    String numericField = poiTileConfig.sortField();
    if (numericField != null) {
      // 如果 numericField 存在，基于该字段排序
      Object fieldValue = tags.get(numericField);
      if (fieldValue instanceof Number number) {
        int numericRank = number.intValue();
        return poiTileConfig.isAsc() ? numericRank : -numericRank;
      }
    } else if (poiTileConfig.priorityLevelsMap() != null) {
      // 否则使用 priorityLevelsMap 逻辑
      return getFieldValueRank(tags, poiTileConfig.priorityLevelsMap());
    }
    return null;
  }

  private Integer getFieldValueMinZoom(Map<String, Object> tags, Map<String, Map<String, Integer>> zoomLevelsMap) {
    if (zoomLevelsMap == null || zoomLevelsMap.isEmpty()) {
      return null;
    }

    return zoomLevelsMap.keySet().stream()
      .map(
        zoomKey -> Optional.ofNullable(tags.get(zoomKey)).map(value -> zoomLevelsMap.get(zoomKey).get(value.toString()))
          .orElse(null))
      .filter(Objects::nonNull)
      .min(Integer::compare)
      .orElse(null);
  }

  private Integer getFieldValueRank(Map<String, Object> tags, Map<String, Map<String, Integer>> priorityLevelsMap) {
    if (priorityLevelsMap == null || priorityLevelsMap.isEmpty()) {
      return null;
    }

    return priorityLevelsMap.keySet().stream()
      .map(priorityKey -> Optional.ofNullable(tags.get(priorityKey))
        .map(value -> priorityLevelsMap.get(priorityKey).getOrDefault(value.toString(), 10000))
        .orElse(10000))
      .min(Integer::compare)
      .orElse(null);
  }

  public record PoiTilingConfig(
    Map<String, Map<String, Integer>> priorityLevelsMap, //数据保留优先级
    Map<String, Map<String, Integer>> zoomLevelsMap, // 数据展示层级
    String sortField, // 排序字段, 优先级高于priorityLevelsMap
    boolean isAsc // 排序排序方式
  ) {}
}
