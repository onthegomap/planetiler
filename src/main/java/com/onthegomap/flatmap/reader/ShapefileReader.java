package com.onthegomap.flatmap.reader;

import com.onthegomap.flatmap.FeatureRenderer;
import com.onthegomap.flatmap.Profile;
import com.onthegomap.flatmap.stats.Stats;
import java.io.File;

public class ShapefileReader extends Reader {

  public ShapefileReader(String sourceProjection, File input, Stats stats) {
    super(stats);
  }

  public ShapefileReader(File input, Stats stats) {
  }

  public void process(FeatureRenderer renderer, Profile profile, int threads) {
  }
}
