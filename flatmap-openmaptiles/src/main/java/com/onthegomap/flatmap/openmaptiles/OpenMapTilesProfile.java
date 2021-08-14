package com.onthegomap.flatmap.openmaptiles;

import static com.onthegomap.flatmap.openmaptiles.Expression.FALSE;
import static com.onthegomap.flatmap.openmaptiles.Expression.TRUE;
import static com.onthegomap.flatmap.openmaptiles.Expression.matchType;

import com.onthegomap.flatmap.FeatureCollector;
import com.onthegomap.flatmap.Profile;
import com.onthegomap.flatmap.Translations;
import com.onthegomap.flatmap.VectorTileEncoder;
import com.onthegomap.flatmap.config.Arguments;
import com.onthegomap.flatmap.geo.GeometryException;
import com.onthegomap.flatmap.openmaptiles.generated.OpenMapTilesSchema;
import com.onthegomap.flatmap.openmaptiles.generated.Tables;
import com.onthegomap.flatmap.reader.ReaderFeature;
import com.onthegomap.flatmap.reader.SourceFeature;
import com.onthegomap.flatmap.reader.osm.OsmElement;
import com.onthegomap.flatmap.reader.osm.OsmReader;
import com.onthegomap.flatmap.stats.Stats;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpenMapTilesProfile implements Profile {

  private static final Logger LOGGER = LoggerFactory.getLogger(OpenMapTilesProfile.class);

  public static final String LAKE_CENTERLINE_SOURCE = "lake_centerlines";
  public static final String WATER_POLYGON_SOURCE = "water_polygons";
  public static final String NATURAL_EARTH_SOURCE = "natural_earth";
  public static final String OSM_SOURCE = "osm";
  private final MultiExpression.MultiExpressionIndex<Tables.Constructor> osmPointMappings;
  private final MultiExpression.MultiExpressionIndex<Tables.Constructor> osmLineMappings;
  private final MultiExpression.MultiExpressionIndex<Tables.Constructor> osmPolygonMappings;
  private final MultiExpression.MultiExpressionIndex<Tables.Constructor> wikidataOsmPointMappings;
  private final MultiExpression.MultiExpressionIndex<Tables.Constructor> wikidataOsmLineMappings;
  private final MultiExpression.MultiExpressionIndex<Tables.Constructor> wikidataOsmPolygonMappings;
  private final List<Layer> layers;
  private final Map<Class<? extends Tables.Row>, List<Tables.RowHandler<Tables.Row>>> osmDispatchMap;
  private final Map<String, FeaturePostProcessor> postProcessors;
  private final List<NaturalEarthProcessor> naturalEarthProcessors;
  private final List<OsmWaterPolygonProcessor> osmWaterProcessors;
  private final List<LakeCenterlineProcessor> lakeCenterlineProcessors;
  private final List<OsmAllProcessor> osmAllProcessors;
  private final List<OsmRelationPreprocessor> osmRelationPreprocessors;
  private final List<FinishHandler> finishHandlers;
  private final Map<Class<? extends Tables.Row>, Set<Class<?>>> osmClassHandlerMap;

  private MultiExpression.MultiExpressionIndex<Tables.Constructor> indexForType(String type, boolean requireWikidata) {
    return Tables.MAPPINGS
      .filterKeys(constructor -> {
        // exclude any mapping that generates a class we don't have a handler for
        var clz = constructor.create(new ReaderFeature(null, Map.of(), 0), "").getClass();
        var handlers = osmClassHandlerMap.getOrDefault(clz, Set.of()).stream();
        if (requireWikidata) {
          handlers = handlers.filter(handler -> !IgnoreWikidata.class.isAssignableFrom(handler));
        }
        return handlers.findAny().isPresent();
      })
      .replace(matchType(type), TRUE)
      .replace(e -> e instanceof Expression.MatchType, FALSE)
      .simplify()
      .index();
  }

  public OpenMapTilesProfile(Translations translations, Arguments arguments, Stats stats) {
    List<String> onlyLayers = arguments.get("only_layers", "Include only certain layers", new String[]{});
    List<String> excludeLayers = arguments.get("exclude_layers", "Exclude certain layers", new String[]{});
    this.layers = OpenMapTilesSchema.createInstances(translations, arguments, stats)
      .stream()
      .filter(l -> (onlyLayers.isEmpty() || onlyLayers.contains(l.name())) && !excludeLayers.contains(l.name()))
      .toList();
    osmDispatchMap = new HashMap<>();
    Tables.generateDispatchMap(layers).forEach((clazz, handlers) -> {
      osmDispatchMap.put(clazz, handlers.stream().map(handler -> {
        @SuppressWarnings("unchecked") Tables.RowHandler<Tables.Row> rawHandler = (Tables.RowHandler<Tables.Row>) handler;
        return rawHandler;
      }).toList());
    });
    osmClassHandlerMap = Tables.generateHandlerClassMap(layers);
    this.osmPointMappings = indexForType("point", false);
    this.osmLineMappings = indexForType("linestring", false);
    this.osmPolygonMappings = indexForType("polygon", false);
    this.wikidataOsmPointMappings = indexForType("point", true);
    this.wikidataOsmLineMappings = indexForType("linestring", true);
    this.wikidataOsmPolygonMappings = indexForType("polygon", true);
    postProcessors = new HashMap<>();
    osmAllProcessors = new ArrayList<>();
    lakeCenterlineProcessors = new ArrayList<>();
    naturalEarthProcessors = new ArrayList<>();
    osmWaterProcessors = new ArrayList<>();
    osmRelationPreprocessors = new ArrayList<>();
    finishHandlers = new ArrayList<>();
    for (Layer layer : layers) {
      if (layer instanceof FeaturePostProcessor postProcessor) {
        postProcessors.put(layer.name(), postProcessor);
      }
      if (layer instanceof OsmAllProcessor processor) {
        osmAllProcessors.add(processor);
      }
      if (layer instanceof OsmWaterPolygonProcessor processor) {
        osmWaterProcessors.add(processor);
      }
      if (layer instanceof LakeCenterlineProcessor processor) {
        lakeCenterlineProcessors.add(processor);
      }
      if (layer instanceof NaturalEarthProcessor processor) {
        naturalEarthProcessors.add(processor);
      }
      if (layer instanceof OsmRelationPreprocessor processor) {
        osmRelationPreprocessors.add(processor);
      }
      if (layer instanceof FinishHandler processor) {
        finishHandlers.add(processor);
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
    List<VectorTileEncoder.Feature> result = null;
    if (postProcesor != null) {
      result = postProcesor.postProcess(zoom, items);
    }
    return result == null ? items : result;
  }

  @Override
  public List<OsmReader.RelationInfo> preprocessOsmRelation(OsmElement.Relation relation) {
    List<OsmReader.RelationInfo> result = null;
    for (int i = 0; i < osmRelationPreprocessors.size(); i++) {
      List<OsmReader.RelationInfo> thisResult = osmRelationPreprocessors.get(i)
        .preprocessOsmRelation(relation);
      if (thisResult != null) {
        if (result == null) {
          result = new ArrayList<>(thisResult);
        } else {
          result.addAll(thisResult);
        }
      }
    }
    return result;
  }

  @Override
  public void processFeature(SourceFeature sourceFeature, FeatureCollector features) {
    switch (sourceFeature.getSource()) {
      case OSM_SOURCE -> {
        for (var match : getTableMatches(sourceFeature)) {
          var row = match.match().create(sourceFeature, match.keys().get(0));
          var handlers = osmDispatchMap.get(row.getClass());
          if (handlers != null) {
            for (Tables.RowHandler<Tables.Row> handler : handlers) {
              handler.process(row, features);
            }
          }
        }
        for (int i = 0; i < osmAllProcessors.size(); i++) {
          osmAllProcessors.get(i).processAllOsm(sourceFeature, features);
        }
      }
      case LAKE_CENTERLINE_SOURCE -> {
        for (LakeCenterlineProcessor lakeCenterlineProcessor : lakeCenterlineProcessors) {
          lakeCenterlineProcessor.processLakeCenterline(sourceFeature, features);
        }
      }
      case NATURAL_EARTH_SOURCE -> {
        for (NaturalEarthProcessor naturalEarthProcessor : naturalEarthProcessors) {
          naturalEarthProcessor.processNaturalEarth(sourceFeature.getSourceLayer(), sourceFeature, features);
        }
      }
      case WATER_POLYGON_SOURCE -> {
        for (OsmWaterPolygonProcessor osmWaterProcessor : osmWaterProcessors) {
          osmWaterProcessor.processOsmWater(sourceFeature, features);
        }
      }
    }
  }

  List<MultiExpression.MultiExpressionIndex.MatchWithTriggers<Tables.Constructor>> getTableMatches(
    SourceFeature sourceFeature) {
    List<MultiExpression.MultiExpressionIndex.MatchWithTriggers<Tables.Constructor>> result = null;
    if (sourceFeature.isPoint()) {
      result = osmPointMappings.getMatchesWithTriggers(sourceFeature.tags());
    } else {
      if (sourceFeature.canBeLine()) {
        result = osmLineMappings.getMatchesWithTriggers(sourceFeature.tags());
        if (sourceFeature.canBePolygon()) {
          result.addAll(osmPolygonMappings.getMatchesWithTriggers(sourceFeature.tags()));
        }
      } else if (sourceFeature.canBePolygon()) {
        result = osmPolygonMappings.getMatchesWithTriggers(sourceFeature.tags());
      }
    }
    return result == null ? List.of() : result;
  }

  @Override
  public void finish(String sourceName, FeatureCollector.Factory featureCollectors,
    Consumer<FeatureCollector.Feature> next) {
    for (var handler : finishHandlers) {
      handler.finish(sourceName, featureCollectors, next);
    }
  }

  @Override
  public boolean caresAboutSource(String name) {
    return switch (name) {
      case NATURAL_EARTH_SOURCE -> !naturalEarthProcessors.isEmpty();
      case WATER_POLYGON_SOURCE -> !osmWaterProcessors.isEmpty();
      case OSM_SOURCE -> !osmAllProcessors.isEmpty() || !osmDispatchMap.isEmpty();
      case LAKE_CENTERLINE_SOURCE -> !lakeCenterlineProcessors.isEmpty();
      default -> true;
    };
  }

  public interface NaturalEarthProcessor {

    void processNaturalEarth(String table, SourceFeature feature, FeatureCollector features);
  }

  public interface LakeCenterlineProcessor {

    void processLakeCenterline(SourceFeature feature, FeatureCollector features);
  }

  public interface OsmWaterPolygonProcessor {

    void processOsmWater(SourceFeature feature, FeatureCollector features);
  }

  public interface OsmAllProcessor {

    void processAllOsm(SourceFeature feature, FeatureCollector features);
  }

  public interface FinishHandler {

    void finish(String sourceName, FeatureCollector.Factory featureCollectors,
      Consumer<FeatureCollector.Feature> next);
  }

  public interface OsmRelationPreprocessor {

    List<OsmReader.RelationInfo> preprocessOsmRelation(OsmElement.Relation relation);
  }

  public interface FeaturePostProcessor {

    List<VectorTileEncoder.Feature> postProcess(int zoom, List<VectorTileEncoder.Feature> items)
      throws GeometryException;
  }

  public interface IgnoreWikidata {}

  @Override
  public boolean caresAboutWikidataTranslation(OsmElement elem) {
    var tags = elem.tags();
    if (elem instanceof OsmElement.Node) {
      return wikidataOsmPointMappings.matches(tags);
    } else if (elem instanceof OsmElement.Way) {
      return wikidataOsmPolygonMappings.matches(tags) || wikidataOsmLineMappings.matches(tags);
    } else if (elem instanceof OsmElement.Relation) {
      return wikidataOsmPolygonMappings.matches(tags);
    } else {
      return false;
    }
  }

  @Override
  public String name() {
    return OpenMapTilesSchema.NAME;
  }

  @Override
  public String description() {
    return OpenMapTilesSchema.DESCRIPTION;
  }

  @Override
  public String attribution() {
    return OpenMapTilesSchema.ATTRIBUTION;
  }

  @Override
  public String version() {
    return OpenMapTilesSchema.VERSION;
  }
}
