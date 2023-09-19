package com.onthegomap.planetiler.util;

import com.onthegomap.planetiler.geo.TileCoord;
import java.util.HashMap;
import java.util.Map;

public class TileWeights {
  private long total = 0;
  private final Map<Integer, Long> byZoom = new HashMap<>();
  private final Map<TileCoord, Long> weights = new HashMap<>();

  public long getWeight(TileCoord coord) {
    return weights.getOrDefault(coord, 0L);
  }

  public long getZoomWeight(int zoom) {
    return byZoom.getOrDefault(zoom, 0L);
  }

  public long getTotalWeight() {
    return total;
  }

  public TileWeights put(TileCoord coord, long i) {
    weights.merge(coord, i, Long::sum);
    byZoom.merge(coord.z(), i, Long::sum);
    total += i;
    return this;
  }
}
