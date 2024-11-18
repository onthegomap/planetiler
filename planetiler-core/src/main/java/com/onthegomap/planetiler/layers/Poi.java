package com.onthegomap.planetiler.layers;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.collection.FeatureGroup;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.reader.SourceFeature;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Poi implements Profile {

  private static final Logger LOGGER = LoggerFactory.getLogger(Poi.class);

  private final PoiTilingConfig poiTileConfig;
  private final PlanetilerConfig config;

  public Poi(PlanetilerConfig config, PoiTilingConfig poiTileConfig) {
    this.poiTileConfig = poiTileConfig;
    this.config = config;
  }

  @Override
  public void processFeature(SourceFeature source, FeatureCollector features) {
    FeatureCollector.Feature feature = features.anyGeometry(config.layerName());
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
  }

  @Override
  public String name() {
    return config.layerName();
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
    }

    if (config.minFeatureSizeOverrides() != null) {
      feature.setMinPixelSizeOverrides(config.minFeatureSizeOverrides());
    }

    if (config.simplifyToleranceOverrides() != null) {
      feature.setPixelToleranceOverrides(config.simplifyToleranceOverrides());
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
        .map(value -> priorityLevelsMap.get(priorityKey).getOrDefault(value.toString(), FeatureGroup.SORT_KEY_MAX))
        .orElse(FeatureGroup.SORT_KEY_MAX))
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
