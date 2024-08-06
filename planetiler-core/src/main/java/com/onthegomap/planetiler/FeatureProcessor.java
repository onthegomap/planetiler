package com.onthegomap.planetiler;

import com.onthegomap.planetiler.reader.SourceFeature;

/**
 * {@link Profile} 的子组件，负责处理来自特征的图层，并在源完成时选择性地进行处理。
 */
@FunctionalInterface
public interface FeatureProcessor<T extends SourceFeature> {

  /**
   * 为地图中应出现的任何输入特征生成输出特征。
   * <p>
   * 多个线程可能会同时为单个数据源调用此方法，因此实现应确保对任何共享数据结构的线程安全访问。
   * 各个数据源是按顺序处理的。
   * <p>
   * 所有 OSM 节点首先被处理，然后是路径（ways），最后是关系（relations）。
   *
   * @param sourceFeature 来自源数据集的输入特征（OSM元素、shapefile元素等）
   * @param features      用于生成输出地图特征的收集器
   */
  void processFeature(T sourceFeature, FeatureCollector features);
}
