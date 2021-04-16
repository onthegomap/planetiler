package com.onthegomap.flatmap.reader;

import com.onthegomap.flatmap.SourceFeature;
import com.onthegomap.flatmap.monitoring.Stats;
import com.onthegomap.flatmap.worker.Topology.SourceStep;
import java.io.File;

public class NaturalEarthReader extends Reader {

  public NaturalEarthReader(File input, File tmpFile, Stats stats) {
    super(stats);
  }

  @Override
  public long getCount() {
    return 0;
  }

  @Override
  public SourceStep<SourceFeature> open() {
    return null;
  }
}
