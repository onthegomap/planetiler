package com.onthegomap.flatmap;

import com.onthegomap.flatmap.collections.MergeSortFeatureMap;
import com.onthegomap.flatmap.stats.Stats;
import com.onthegomap.flatmap.worker.Worker.WorkerSink;

public class FeatureRenderer {

  public FeatureRenderer(MergeSortFeatureMap featureMap, Stats stats) {

  }

  public WorkerSink<RenderedFeature> newWriterQueue(String name) {
  }

  public void renderFeature(RenderableFeature renderable) {
  }
}
