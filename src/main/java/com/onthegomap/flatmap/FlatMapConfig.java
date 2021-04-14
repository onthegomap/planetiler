package com.onthegomap.flatmap;

import com.onthegomap.flatmap.stats.Stats;
import java.time.Duration;
import org.locationtech.jts.geom.Envelope;

public record FlatMapConfig(
  Profile profile,
  Envelope envelope,
  int threads,
  Stats stats,
  Duration logInterval
) {

}
