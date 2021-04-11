package com.onthegomap.flatmap.collections;

import com.onthegomap.flatmap.RenderedFeature;
import com.onthegomap.flatmap.stats.Stats;
import java.nio.file.Path;
import java.util.function.Consumer;

public class MergeSortFeatureMap implements Consumer<RenderedFeature> {

  public MergeSortFeatureMap(Path featureDb, Stats stats) {

  }

  public void sort() {
  }

  @Override
  public void accept(RenderedFeature renderedFeature) {

  }

  public long getStorageSize() {
    return 0;
  }
}
