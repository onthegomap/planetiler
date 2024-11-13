package com.onthegomap.planetiler.render;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 缓存并管理不同规格的 GridEntity 实例。
 */
public class GridEntityCache {

  private final Map<Integer, GridEntity> cache = new ConcurrentHashMap<>();

  /**
   * 获取或创建指定规格的 GridEntity 实例。
   *
   * @param gridSize 网格大小
   * @return 对应 gridSize 的 GridEntity 实例
   */
  public GridEntity getOrCreateGridEntity(int gridSize) {
    return cache.computeIfAbsent(gridSize, GridEntity::new);
  }

  /**
   * 清空缓存。
   */
  public void clearCache() {
    cache.clear();
  }

  /**
   * 返回缓存的所有 GridEntity 实例。
   */
  public Map<Integer, GridEntity> getAllCachedEntities() {
    return cache;
  }
}
