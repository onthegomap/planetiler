package com.onthegomap.flatmap.reader;

import com.onthegomap.flatmap.FeatureRenderer;
import com.onthegomap.flatmap.OsmInputFile;
import com.onthegomap.flatmap.profiles.OpenMapTilesProfile;
import com.onthegomap.flatmap.stats.Stats;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;

public class OpenStreetMapReader implements Closeable {

  public OpenStreetMapReader(OsmInputFile osmInputFile, File nodeDb, Stats stats) {
  }

  public void pass1(OpenMapTilesProfile profile, int threads) {
  }

  public void pass2(FeatureRenderer renderer, OpenMapTilesProfile profile, int threads) {
  }

  @Override
  public void close() throws IOException {

  }

  public static class RelationInfo {

  }
}
