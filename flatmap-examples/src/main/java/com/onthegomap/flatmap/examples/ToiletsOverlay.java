package com.onthegomap.flatmap.examples;

import com.onthegomap.flatmap.FeatureCollector;
import com.onthegomap.flatmap.FlatMapRunner;
import com.onthegomap.flatmap.Profile;
import com.onthegomap.flatmap.reader.SourceFeature;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ToiletsOverlay implements Profile {

  private static final Logger LOGGER = LoggerFactory.getLogger(ToiletsOverlay.class);

  AtomicInteger toiletNumber = new AtomicInteger(0);

  @Override
  public void processFeature(SourceFeature sourceFeature, FeatureCollector features) {
    if (sourceFeature.hasTag("amenity", "toilets")) {
      features.centroid("toilets")
        .setZoomRange(0, 14)
        .setZorder(toiletNumber.incrementAndGet())
        .setLabelGridSizeAndLimit(12, 32, 4);
    }
  }

  @Override
  public String name() {
    return "Toilets Overlay";
  }

  @Override
  public String description() {
    return "An example overlay showing toilets";
  }

  @Override
  public boolean isOverlay() {
    return true;
  }

  @Override
  public String attribution() {
    return """
      <a href="https://www.openstreetmap.org/copyright" target="_blank">&copy; OpenStreetMap contributors</a>
      """.trim();
  }

  public static void main(String[] args) throws Exception {
    FlatMapRunner.create()
      .setProfile(new ToiletsOverlay())
      .addOsmSource("osm", Path.of("data", "sources", "north-america_us_massachusetts.pbf"))
      .overwriteOutput("mbtiles", Path.of("data", "toilets.mbtiles"))
      .run();
  }
}
