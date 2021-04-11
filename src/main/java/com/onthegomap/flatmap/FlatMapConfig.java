package com.onthegomap.flatmap;

import com.onthegomap.flatmap.profiles.OpenMapTilesProfile;
import com.onthegomap.flatmap.stats.Stats;
import org.locationtech.jts.geom.Envelope;

public record FlatMapConfig(
  OpenMapTilesProfile profile,
  Envelope envelope,
  int threads,
  Stats stats,
  long logIntervalSeconds
) {

}
