package com.onthegomap.flatmap.collections;

import com.carrotsearch.hppc.LongArrayList;
import com.onthegomap.flatmap.RenderedFeature;
import com.onthegomap.flatmap.VectorTileEncoder;
import com.onthegomap.flatmap.geo.TileCoord;
import com.onthegomap.flatmap.monitoring.Stats;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

public class MergeSortFeatureMap implements Consumer<RenderedFeature> {

  private volatile boolean prepared = false;

  public MergeSortFeatureMap(Path featureDb, Stats stats) {

  }

  public void sort() {
    prepared = true;
  }

  @Override
  public void accept(RenderedFeature renderedFeature) {
    if (prepared) {
      throw new IllegalStateException("Attempting to add feature but already prepared");
    }
  }

  public long getStorageSize() {
    return 0;
  }

  public Iterator<TileFeatures> getAll() {
    if (!prepared) {
      throw new IllegalStateException("Attempting to iterate over features but not prepared yet");
    }
    return new Iterator<>() {
      @Override
      public boolean hasNext() {
        return false;
      }

      @Override
      public TileFeatures next() {
        return null;
      }
    };
  }

  public static class TileFeatures {

    private final TileCoord tile;
    private final LongArrayList sortKeys = new LongArrayList();
    private final List<byte[]> entries = new ArrayList<>();

    public TileFeatures(int tile) {
      this.tile = TileCoord.decode(tile);
    }

    public long getNumFeatures() {
      return 0;
    }

    public TileCoord coord() {
      return tile;
    }

    public boolean hasSameContents(TileFeatures other) {
      return false;
    }

    public VectorTileEncoder getTile() {
      return null;
    }

    @Override
    public String toString() {
      return "TileFeatures{" +
        "tile=" + tile +
        ", sortKeys=" + sortKeys +
        ", entries=" + entries +
        '}';
    }
  }
}
