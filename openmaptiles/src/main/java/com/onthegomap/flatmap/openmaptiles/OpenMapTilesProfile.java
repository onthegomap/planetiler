package com.onthegomap.flatmap.openmaptiles;

import com.graphhopper.reader.ReaderRelation;
import com.onthegomap.flatmap.Arguments;
import com.onthegomap.flatmap.FeatureCollector;
import com.onthegomap.flatmap.Profile;
import com.onthegomap.flatmap.SourceFeature;
import com.onthegomap.flatmap.Translations;
import com.onthegomap.flatmap.VectorTileEncoder;
import com.onthegomap.flatmap.geo.GeometryException;
import com.onthegomap.flatmap.monitoring.Stats;
import com.onthegomap.flatmap.openmaptiles.generated.Layers;
import com.onthegomap.flatmap.openmaptiles.generated.Tables;
import com.onthegomap.flatmap.read.OpenStreetMapReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpenMapTilesProfile implements Profile {

  private static final boolean MERGE_Z13_BUILDINGS = false;

  public static final String LAKE_CENTERLINE_SOURCE = "lake_centerlines";
  public static final String WATER_POLYGON_SOURCE = "water_polygons";
  public static final String NATURAL_EARTH_SOURCE = "natural_earth";
  public static final String OSM_SOURCE = "osm";
  private static final Logger LOGGER = LoggerFactory.getLogger(OpenMapTilesProfile.class);
  private final MultiExpression.MultiExpressionIndex<Tables.Constructor> osmMappings;
  private final List<Layer> layers;
  private final Map<Class<? extends Tables.Row>, List<Tables.RowHandler<Tables.Row>>> osmDispatchMap;
  private final Map<String, FeaturePostProcessor> postProcessors;

  public OpenMapTilesProfile(Translations translations, Arguments arguments, Stats stats) {
    this.osmMappings = Tables.MAPPINGS.index();
    this.layers = Layers.createInstances(translations, arguments, stats);
    osmDispatchMap = new HashMap<>();
    Tables.generateDispatchMap(layers).forEach((clazz, handlers) -> {
      osmDispatchMap.put(clazz, handlers.stream().map(handler -> {
        @SuppressWarnings("unchecked") Tables.RowHandler<Tables.Row> rawHandler = (Tables.RowHandler<Tables.Row>) handler;
        return rawHandler;
      }).toList());
    });
    postProcessors = new HashMap<>();
    for (Layer layer : layers) {
      if (layer instanceof FeaturePostProcessor postProcessor) {
        postProcessors.put(layer.name(), postProcessor);
      }
    }
  }

  @Override
  public void release() {
    layers.forEach(Layer::release);
  }

  @Override
  public List<VectorTileEncoder.Feature> postProcessLayerFeatures(String layer, int zoom,
    List<VectorTileEncoder.Feature> items) throws GeometryException {
    FeaturePostProcessor postProcesor = postProcessors.get(layer);
    List<VectorTileEncoder.Feature> result = postProcesor.postProcess(zoom, items);
//    if (MERGE_Z13_BUILDINGS && "building".equals(layer) && zoom == 13) {
//      return FeatureMerge.mergePolygons(items, 4, 0.5, 0.5);
//    }
    return result == null ? items : result;
  }

  @Override
  public List<OpenStreetMapReader.RelationInfo> preprocessOsmRelation(ReaderRelation relation) {
    return null;
  }

  @Override
  public void processFeature(SourceFeature sourceFeature, FeatureCollector features) {
    if (OSM_SOURCE.equals(sourceFeature.getSource())) {
      if (sourceFeature.canBeLine()) {
        sourceFeature.properties().put("__linestring", "true");
      }
      if (sourceFeature.canBePolygon()) {
        sourceFeature.properties().put("__polygon", "true");
      }
      if (sourceFeature.isPoint()) {
        sourceFeature.properties().put("__point", "true");
      }
      for (var match : osmMappings.getMatchesWithTriggers(sourceFeature.properties())) {
        var row = match.match().create(sourceFeature, match.keys().get(0));
        var handlers = osmDispatchMap.get(row.getClass());
        if (handlers != null) {
          for (Tables.RowHandler<Tables.Row> handler : handlers) {
            handler.process(row, features);
          }
        }
      }
    }
//
//    if (sourceFeature.isPoint()) {
//      if (sourceFeature.hasTag("natural", "peak", "volcano")) {
//        features.point("mountain_peak")
//          .setAttr("name", sourceFeature.getTag("name"))
//          .setLabelGridSizeAndLimit(13, 100, 5);
//      }
//    }
//
//    if (WATER_POLYGON_SOURCE.equals(sourceFeature.getSource())) {
//      features.polygon("water").setZoomRange(6, 14).setAttr("class", "ocean");
//    } else if (NATURAL_EARTH_SOURCE.equals(sourceFeature.getSource())) {
//      String sourceLayer = sourceFeature.getSourceLayer();
//      boolean lake = sourceLayer.endsWith("_lakes");
//      switch (sourceLayer) {
//        case "ne_10m_lakes", "ne_10m_ocean" -> features.polygon("water")
//          .setZoomRange(4, 5)
//          .setAttr("class", lake ? "lake" : "ocean");
//        case "ne_50m_lakes", "ne_50m_ocean" -> features.polygon("water")
//          .setZoomRange(2, 3)
//          .setAttr("class", lake ? "lake" : "ocean");
//        case "ne_110m_lakes", "ne_110m_ocean" -> features.polygon("water")
//          .setZoomRange(0, 1)
//          .setAttr("class", lake ? "lake" : "ocean");
//      }
//    }
//
//    if (OSM_SOURCE.equals(sourceFeature.getSource())) {
//      if (sourceFeature.canBePolygon()) {
//        if (sourceFeature.hasTag("building")) {
//          features.polygon("building")
//            .setZoomRange(13, 14)
//            .setMinPixelSize(MERGE_Z13_BUILDINGS ? 0 : 4);
//        }
//      }
//    }
  }

  public interface SourceFeatureProcessors {

    void process(SourceFeature feature, FeatureCollector features);
  }

  public interface FeaturePostProcessor {

    List<VectorTileEncoder.Feature> postProcess(int zoom, List<VectorTileEncoder.Feature> items)
      throws GeometryException;
  }

  @Override
  public String name() {
    return Layers.NAME;
  }

  @Override
  public String description() {
    return Layers.DESCRIPTION;
  }

  @Override
  public String attribution() {
    return Layers.ATTRIBUTION;
  }

  @Override
  public String version() {
    return Layers.VERSION;
  }
}
