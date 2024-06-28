package com.onthegomap.planetiler;

import com.onthegomap.planetiler.reader.SourceFeature;

/**
 * Subcomponent of {@link Profile} that handles processing layers from a feature, and optionally when that source is
 * finished.
 */
@FunctionalInterface
public interface FeatureProcessor<T extends SourceFeature> {

  /**
   * Generates output features for any input feature that should appear in the map.
   * <p>
   * Multiple threads may invoke this method concurrently for a single data source so implementations should ensure
   * thread-safe access to any shared data structures. Separate data sources are processed sequentially.
   * <p>
   * All OSM nodes are processed first, then ways, then relations.
   *
   * @param sourceFeature the input feature from a source dataset (OSM element, shapefile element, etc.)
   * @param features      a collector for generating output map features to emit
   */
  void processFeature(T sourceFeature, FeatureCollector features);
}
