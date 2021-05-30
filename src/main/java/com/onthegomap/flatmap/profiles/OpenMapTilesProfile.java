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

  public static final String LAKE_CENTERLINE_SOURCE = "lake_centerlines";
  public static final String WATER_POLYGON_SOURCE = "water_polygons";
  public static final String NATURAL_EARTH_SOURCE = "natural_earth";
  public static final String OSM_SOURCE = "osm";
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
  public void processFeature(SourceFeature sourceFeature, FeatureCollector features) {
    if (sourceFeature.isPoint()) {
      if (sourceFeature.hasTag("natural", "peak", "volcano")) {
        features.point("mountain_peak")
          .setAttr("name", sourceFeature.getTag("name"))
          .setLabelGridSizeAndLimit(13, 100, 5);
      }
    }

    if (WATER_POLYGON_SOURCE.equals(sourceFeature.getSource())) {
      features.polygon("water").setZoomRange(6, 14).setAttr("class", "ocean");
    } else if (NATURAL_EARTH_SOURCE.equals(sourceFeature.getSource())) {
      String sourceLayer = sourceFeature.getSourceLayer();
      boolean lake = sourceLayer.endsWith("_lakes");
      switch (sourceLayer) {
        case "ne_10m_lakes", "ne_10m_ocean" -> features.polygon("water")
          .setZoomRange(4, 5)
          .setAttr("class", lake ? "lake" : "ocean");
        case "ne_50m_lakes", "ne_50m_ocean" -> features.polygon("water")
          .setZoomRange(2, 3)
          .setAttr("class", lake ? "lake" : "ocean");
        case "ne_110m_lakes", "ne_110m_ocean" -> features.polygon("water")
          .setZoomRange(0, 1)
          .setAttr("class", lake ? "lake" : "ocean");
      }
    }

    if (OSM_SOURCE.equals(sourceFeature.getSource())) {
      if (sourceFeature.canBePolygon()) {
        if (sourceFeature.hasTag("building")) {
          features.polygon("building")
            .setZoomRange(13, 14)
            .setMinPixelSize(4);
        }
      }
    }
  }
}
