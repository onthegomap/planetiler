package com.onthegomap.planetiler.basemap;

import static com.onthegomap.planetiler.geo.GeoUtils.EMPTY_LINE;
import static com.onthegomap.planetiler.geo.GeoUtils.EMPTY_POINT;
import static com.onthegomap.planetiler.geo.GeoUtils.EMPTY_POLYGON;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.ForwardingProfile;
import com.onthegomap.planetiler.Planetiler;
import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.basemap.generated.OpenMapTilesSchema;
import com.onthegomap.planetiler.basemap.generated.Tables;
import com.onthegomap.planetiler.basemap.layers.Transportation;
import com.onthegomap.planetiler.basemap.layers.TransportationName;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.expression.MultiExpression;
import com.onthegomap.planetiler.reader.SimpleFeature;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.reader.osm.OsmElement;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.util.Translations;
import java.util.ArrayList;
import java.util.List;

/**
 * Delegates the logic for generating a map to individual implementations in the {@code layers} package.
 * <p>
 * Layer implementations extend these interfaces to subscribe to elements from different sources:
 * <ul>
 *   <li>{@link LakeCenterlineProcessor}</li>
 *   <li>{@link NaturalEarthProcessor}</li>
 *   <li>{@link OsmWaterPolygonProcessor}</li>
 *   <li>{@link OsmAllProcessor} to process every OSM feature</li>
 *   <li>{@link OsmRelationPreprocessor} to process every OSM relation during first pass through OSM file</li>
 *   <li>A {@link Tables.RowHandler} implementation in {@code Tables.java} to process input features filtered and parsed
 *   according to the imposm3 mappings defined in the OpenMapTiles schema. Each element corresponds to a row in the
 *   table that imposm3 would have generated, with generated methods for accessing the data that would have been in each
 *   column</li>
 * </ul>
 * Layers can also subscribe to notifications when we finished processing an input source by implementing
 * {@link FinishHandler} or post-process features in that layer before rendering the output tile by implementing
 * {@link FeaturePostProcessor}.
 */
public class BasemapProfile extends ForwardingProfile {

  // IDs used in stats and logs for each input source, as well as argument/config file overrides to source locations
  public static final String LAKELINES_SOURCE = "lakelines";
  public static final String WATER_POLYGON_SOURCE = "water_polygons";
  public static final String NATURAL_EARTH_SOURCE = "natural_earth";
  public static final String OSM_SOURCE = "osm";
  /** Index to efficiently find the imposm3 "table row" constructor from an OSM element based on its tags. */
  private final MultiExpression.Index<RowDispatch> osmMappings;
  /** Index variant that filters out any table only used by layers that implement IgnoreWikidata class. */
  private final MultiExpression.Index<Boolean> wikidataMappings;

  public BasemapProfile(Planetiler runner) {
    this(runner.translations(), runner.config(), runner.stats());
  }

  public BasemapProfile(Translations translations, PlanetilerConfig config, Stats stats) {
    List<String> onlyLayers = config.arguments().getList("only_layers", "Include only certain layers", List.of());
    List<String> excludeLayers = config.arguments().getList("exclude_layers", "Exclude certain layers", List.of());

    // register release/finish/feature postprocessor/osm relationship handler methods...
    List<Handler> layers = new ArrayList<>();
    Transportation transportationLayer = null;
    TransportationName transportationNameLayer = null;
    for (Layer layer : OpenMapTilesSchema.createInstances(translations, config, stats)) {
      if ((onlyLayers.isEmpty() || onlyLayers.contains(layer.name())) && !excludeLayers.contains(layer.name())) {
        layers.add(layer);
        registerHandler(layer);
        if (layer instanceof TransportationName transportationName) {
          transportationNameLayer = transportationName;
        }
      }
      if (layer instanceof Transportation transportation) {
        transportationLayer = transportation;
      }
    }

    // special-case: transportation_name layer depends on transportation layer
    if (transportationNameLayer != null) {
      transportationNameLayer.needsTransportationLayer(transportationLayer);
      if (!layers.contains(transportationLayer)) {
        layers.add(transportationLayer);
        registerHandler(transportationLayer);
      }
    }

    // register per-source input element handlers
    for (Handler handler : layers) {
      if (handler instanceof NaturalEarthProcessor processor) {
        registerSourceHandler(NATURAL_EARTH_SOURCE,
          (source, features) -> processor.processNaturalEarth(source.getSourceLayer(), source, features));
      }
      if (handler instanceof OsmWaterPolygonProcessor processor) {
        registerSourceHandler(WATER_POLYGON_SOURCE, processor::processOsmWater);
      }
      if (handler instanceof LakeCenterlineProcessor processor) {
        registerSourceHandler(LAKELINES_SOURCE, processor::processLakeCenterline);
      }
      if (handler instanceof OsmAllProcessor processor) {
        registerSourceHandler(OSM_SOURCE, processor::processAllOsm);
      }
    }

    // pre-process layers to build efficient indexes for matching OSM elements based on matching expressions
    // Map from imposm3 table row class to the layers that implement its handler.
    var handlerMap = Tables.generateDispatchMap(layers);
    osmMappings = Tables.MAPPINGS
      .mapResults(constructor -> {
        var handlers = handlerMap.getOrDefault(constructor.rowClass(), List.of()).stream()
          .map(r -> {
            @SuppressWarnings("unchecked") var handler = (Tables.RowHandler<Tables.Row>) r.handler();
            return handler;
          })
          .toList();
        return new RowDispatch(constructor.create(), handlers);
      }).simplify().index();
    wikidataMappings = Tables.MAPPINGS
      .mapResults(constructor ->
        handlerMap.getOrDefault(constructor.rowClass(), List.of()).stream()
          .anyMatch(handler -> !IgnoreWikidata.class.isAssignableFrom(handler.handlerClass()))
      ).filterResults(b -> b).simplify().index();

    // register a handler for all OSM elements that forwards to imposm3 "table row" handler methods
    // based on efficient pre-processed index
    if (!osmMappings.isEmpty()) {
      registerSourceHandler(OSM_SOURCE, (source, features) -> {
        for (var match : getTableMatches(source)) {
          RowDispatch rowDispatch = match.match();
          var row = rowDispatch.constructor.create(source, match.keys().get(0));
          for (Tables.RowHandler<Tables.Row> handler : rowDispatch.handlers()) {
            handler.process(row, features);
          }
        }
      });
    }
  }

  /** Returns the imposm3 table row constructors that match an input element's tags. */
  public List<MultiExpression.Match<RowDispatch>> getTableMatches(SourceFeature input) {
    return osmMappings.getMatchesWithTriggers(input);
  }

  @Override
  public boolean caresAboutWikidataTranslation(OsmElement elem) {
    var tags = elem.tags();
    if (elem instanceof OsmElement.Node) {
      return wikidataMappings.getOrElse(SimpleFeature.create(EMPTY_POINT, tags), false);
    } else if (elem instanceof OsmElement.Way) {
      return wikidataMappings.getOrElse(SimpleFeature.create(EMPTY_POLYGON, tags), false)
        || wikidataMappings.getOrElse(SimpleFeature.create(EMPTY_LINE, tags), false);
    } else if (elem instanceof OsmElement.Relation) {
      return wikidataMappings.getOrElse(SimpleFeature.create(EMPTY_POLYGON, tags), false);
    } else {
      return false;
    }
  }

  /*
   * Pass-through constants generated from the OpenMapTiles vector schema
   */

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

  /**
   * Layers should implement this interface to subscribe to elements from <a href="https://www.naturalearthdata.com/">natural
   * earth</a>.
   */
  public interface NaturalEarthProcessor {

    /**
     * Process an element from {@code table} in the<a href="https://www.naturalearthdata.com/">natural earth
     * source</a>.
     *
     * @see Profile#processFeature(SourceFeature, FeatureCollector)
     */
    void processNaturalEarth(String table, SourceFeature feature, FeatureCollector features);
  }

  /**
   * Layers should implement this interface to subscribe to elements from <a href="https://github.com/lukasmartinelli/osm-lakelines">OSM
   * lake centerlines source</a>.
   */
  public interface LakeCenterlineProcessor {

    /**
     * Process an element from the <a href="https://github.com/lukasmartinelli/osm-lakelines">OSM lake centerlines
     * source</a>
     *
     * @see Profile#processFeature(SourceFeature, FeatureCollector)
     */
    void processLakeCenterline(SourceFeature feature, FeatureCollector features);
  }

  /**
   * Layers should implement this interface to subscribe to elements from <a href="https://osmdata.openstreetmap.de/data/water-polygons.html">OSM
   * water polygons source</a>.
   */
  public interface OsmWaterPolygonProcessor {

    /**
     * Process an element from the <a href="https://osmdata.openstreetmap.de/data/water-polygons.html">OSM water
     * polygons source</a>
     *
     * @see Profile#processFeature(SourceFeature, FeatureCollector)
     */
    void processOsmWater(SourceFeature feature, FeatureCollector features);
  }

  /** Layers should implement this interface to subscribe to every OSM element. */
  public interface OsmAllProcessor {

    /**
     * Process an OSM element during the second pass through the OSM data file.
     *
     * @see Profile#processFeature(SourceFeature, FeatureCollector)
     */
    void processAllOsm(SourceFeature feature, FeatureCollector features);
  }

  /**
   * Layers should implement to indicate they do not need wikidata name translations to avoid downloading more
   * translations than are needed.
   */
  public interface IgnoreWikidata {}

  private record RowDispatch(
    Tables.Constructor constructor,
    List<Tables.RowHandler<Tables.Row>> handlers
  ) {}
}
