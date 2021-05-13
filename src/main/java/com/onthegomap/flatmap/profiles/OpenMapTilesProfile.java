package com.onthegomap.flatmap.profiles;

import com.graphhopper.reader.ReaderRelation;
import com.onthegomap.flatmap.FeatureCollector;
import com.onthegomap.flatmap.Profile;
import com.onthegomap.flatmap.SourceFeature;
import com.onthegomap.flatmap.VectorTileEncoder;
import com.onthegomap.flatmap.read.OpenStreetMapReader;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpenMapTilesProfile implements Profile {

  private static final Logger LOGGER = LoggerFactory.getLogger(OpenMapTilesProfile.class);

  @Override
  public void release() {
  }

  @Override
  public List<VectorTileEncoder.Feature> postProcessLayerFeatures(String layer, int zoom,
    List<VectorTileEncoder.Feature> items) {
    return items;
  }

  @Override
  public String name() {
    return "OpenMapTiles";
  }

  @Override
  public String description() {
    return "A tileset showcasing all layers in OpenMapTiles. https://openmaptiles.org";
  }

  @Override
  public String attribution() {
    return """
      <a href="https://www.openmaptiles.org/" target="_blank">&copy; OpenMapTiles</a> <a href="https://www.openstreetmap.org/copyright" target="_blank">&copy; OpenStreetMap contributors</a>
      """.trim();
  }

  @Override
  public String version() {
    return "3.12.1";
  }

  @Override
  public List<OpenStreetMapReader.RelationInfo> preprocessOsmRelation(ReaderRelation relation) {
    return null;
  }

  @Override
  public void processFeature(SourceFeature sourceFeature,
    FeatureCollector features) {
    if (sourceFeature.isPoint()) {
      if (sourceFeature.hasTag("natural", "peak", "volcano")) {
        features.point("mountain_peak")
          .setAttr("name", sourceFeature.getTag("name"));
      }
    }
  }
}
