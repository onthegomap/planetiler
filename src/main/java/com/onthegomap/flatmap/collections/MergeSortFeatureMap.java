package com.onthegomap.flatmap.collections;

import com.onthegomap.flatmap.RenderedFeature;
import com.onthegomap.flatmap.VectorTile;
import com.onthegomap.flatmap.monitoring.Stats;
import java.nio.file.Path;
import java.util.Iterator;
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

  public Iterator<TileFeatures> getAll() {
    return null;
  }

  public static class TileFeatures {

    public long getNumFeatures() {
      return 0;
    }

    public int getTileId() {
      return 0;
    }

    public boolean hasSameContents(TileFeatures other) {
      return false;
    }

    public VectorTile getTile() {
      return null;
    }
  }
}
