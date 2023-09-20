package com.onthegomap.planetiler.util;

import com.onthegomap.planetiler.geo.TileCoord;
import java.util.HashMap;
import java.util.Map;

/**
 * Holds tile weights to compute weighted average tile sizes.
 * <p>
 * {@link TopOsmTiles} can be used to get tile weights from 90 days of openstreetmap.org tile traffic.
 */
public class TileWeights {
  private final Map<Integer, Long> byZoom = new HashMap<>();
  private final Map<TileCoord, Long> weights = new HashMap<>();

  public long getWeight(TileCoord coord) {
    return weights.getOrDefault(coord, 0L);
  }

  /** Returns the sum of all tile weights at a specific zoom */
  public long getZoomWeight(int zoom) {
    return byZoom.getOrDefault(zoom, 0L);
  }

  /** Adds {@code weight} to the current weight for {@code coord} and returns this modified instance. */
  public TileWeights put(TileCoord coord, long weight) {
    weights.merge(coord, weight, Long::sum);
    byZoom.merge(coord.z(), weight, Long::sum);
    return this;
  }
}
