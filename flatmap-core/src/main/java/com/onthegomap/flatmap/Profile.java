package com.onthegomap.flatmap;

import com.onthegomap.flatmap.geo.GeometryException;
import com.onthegomap.flatmap.mbiles.Mbtiles;
import com.onthegomap.flatmap.reader.SourceFeature;
import com.onthegomap.flatmap.reader.osm.OsmElement;
import com.onthegomap.flatmap.reader.osm.OsmRelationInfo;
import com.onthegomap.flatmap.util.Wikidata;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Provides methods for implementations to control how maps are generated.
 * <p>
 * This includes:
 * <ul>
 *   <li>How source features (OSM elements, shapefile elements, etc.) map to output features and their tags</li>
 *   <li>How vector tile features in an output tile should be post-processed (see {@link FeatureMerge})</li>
 *   <li>What attributes to include in the mbtiles metadata output (see {@link Mbtiles})</li>
 *   <li>Whether {@link Wikidata} class should fetch wikidata translations for an OSM element</li>
 * </ul>
 * <p>
 * {@link Profile#processFeature(SourceFeature, FeatureCollector)} only handles a single element at a time. To "join"
 * elements across or within sources, implementations can store data in instance fields and wait to process them until
 * an element is encountered in a later source, or the {@link Profile#finish(String, FeatureCollector.Factory, Consumer)}
 * method is called for a source. All methods may be called concurrently by multiple threads, so implementations must be
 * careful to ensure access to instance fields is thread-safe.
 * <p>
 * For complex profiles, {@link ForwardingProfile} provides a framework for splitting the logic up into several handlers
 * (i.e. one per layer) and forwarding each element/event to the handlers that care about it.
 */
public interface Profile {

  /**
   * Extracts information from <a href="https://wiki.openstreetmap.org/wiki/Relation">OSM relations</a> that will be
   * passed along to {@link #processFeature(SourceFeature, FeatureCollector)} for any OSM element in that relation.
   * <p>
   * The result of this method is stored in memory.
   *
   * @param relation the OSM relation
   * @return a list of relation info instances with information extracted from the relation to pass to {@link
   * #processFeature(SourceFeature, FeatureCollector)}, or {@code null} to ignore.
   * @implNote The default implementation returns {@code null} to ignore all relations
   */
  default List<OsmRelationInfo> preprocessOsmRelation(OsmElement.Relation relation) {
    return null;
  }

  /**
   * Generates output features for any input feature that should appear in the map.
   * <p>
   * Multiple threads may invoke this method concurrently for a single data source so implementations should ensure
   * thread-safe access to any shared data structures.  Separate data sources are processed sequentially.
   * <p>
   * All OSM nodes are processed first, then ways, then relations.
   *
   * @param sourceFeature the input feature from a source dataset (OSM element, shapefile element, etc.)
   * @param features      a collector for generating output map features to emit
   */
  void processFeature(SourceFeature sourceFeature, FeatureCollector features);

  /** Free any resources associated with this profile (i.e. shared data structures) */
  default void release() {
  }

  /**
   * Apply any post-processing to features in an output layer of a tile before writing it to the output file
   * <p>
   * These transformations may add, remove, or change the tags, geometry, or ordering of output features based on other
   * features present in this tile. See {@link FeatureMerge} class for a set of common transformations that merge
   * linestrings/polygons.
   * <p>
   * Many threads invoke this method concurrently so ensure thread-safe access to any shared data structures.
   *
   * @param layer the output layer name
   * @param zoom  zoom level of the tile
   * @param items all the output features in this layer in this tile
   * @return the new list of output features or {@code null} to not change anything.  Set any elements of the list to
   * {@code null} if they should be ignored.
   * @throws GeometryException for any recoverable geometric operation failures - the framework will log the error, emit
   *                           the original input features, and continue processing other tiles
   * @implSpec The default implementation passes through input features unaltered
   */
  default List<VectorTile.Feature> postProcessLayerFeatures(String layer, int zoom,
      List<VectorTile.Feature> items) throws GeometryException {
    return items;
  }

  /**
   * Returns the name of the generated tileset to put into {@link Mbtiles} metadata
   *
   * @see <a href="https://github.com/mapbox/mbtiles-spec/blob/master/1.3/spec.md#metadata">MBTiles specification</a>
   */
  String name();

  /**
   * Returns the description of the generated tileset to put into {@link Mbtiles} metadata
   *
   * @see <a href="https://github.com/mapbox/mbtiles-spec/blob/master/1.3/spec.md#metadata">MBTiles specification</a>
   */
  default String description() {
    return null;
  }

  /**
   * Returns the attribution of the generated tileset to put into {@link Mbtiles} metadata
   *
   * @see <a href="https://www.openstreetmap.org/copyright">https://www.openstreetmap.org/copyright</a> for attribution
   * requirements of any map using OpenStreetMap data
   * @see <a href="https://github.com/mapbox/mbtiles-spec/blob/master/1.3/spec.md#metadata">MBTiles specification</a>
   */
  default String attribution() {
    return null;
  }

  /**
   * Returns the version of the generated tileset to put into {@link Mbtiles} metadata
   *
   * @see <a href="https://github.com/mapbox/mbtiles-spec/blob/master/1.3/spec.md#metadata">MBTiles specification</a>
   */
  default String version() {
    return null;
  }

  /**
   * Returns {@code true} to set {@code type="overlay"} in {@link Mbtiles} metadata otherwise sets {@code
   * type="baselayer"}
   *
   * @implSpec The default implementation sets {@code type="baselayer"}
   * @see <a href="https://github.com/mapbox/mbtiles-spec/blob/master/1.3/spec.md#metadata">MBTiles specification</a>
   */
  default boolean isOverlay() {
    return false;
  }

  /**
   * Defines whether {@link Wikidata} should fetch wikidata translations for the input element.
   *
   * @param elem the input OSM element
   * @return {@code true} to fetch wikidata translations for {@code elem}, {@code false} to ignore
   * @implSpec the default implementation returns {@code true} for all elements
   */
  default boolean caresAboutWikidataTranslation(OsmElement elem) {
    return true;
  }

  /**
   * Invoked once for each source after all elements for that source have been processed.
   *
   * @param sourceName        the name of the source that just finished
   * @param featureCollectors a supplier for new {@link FeatureCollector} instances for a {@link SourceFeature}.
   * @param next              a consumer to pass finished map features to
   */
  default void finish(String sourceName, FeatureCollector.Factory featureCollectors,
      Consumer<FeatureCollector.Feature> next) {
  }

  /**
   * Returns true if this profile will use any of the elements from an input source.
   *
   * @param name the input source name
   * @return {@code true} if this profile uses that source, {@code false} if it is safe to ignore
   * @implSpec the default implementation returns true
   */
  default boolean caresAboutSource(String name) {
    return true;
  }

  /**
   * A default implementation of {@link Profile} that emits no output elements.
   */
  class NullProfile implements Profile {

    @Override
    public void processFeature(SourceFeature sourceFeature, FeatureCollector features) {
    }

    @Override
    public List<VectorTile.Feature> postProcessLayerFeatures(String layer, int zoom,
        List<VectorTile.Feature> items) {
      return items;
    }

    @Override
    public String name() {
      return "Null";
    }
  }

  /**
   * A profile that delegates handling of input features to individual {@link Handler LayerHandlers} if they implement
   * {@link OsmRelationPreprocessor}, {@link FeatureProcessor}, {@link FinishHandler}, or {@link FeaturePostProcessor}.
   */
  abstract class ForwardingProfile implements Profile {

    private final List<Handler> handlers = new ArrayList<>();
    /** Handlers that pre-process OSM relations during pass 1 through the data. */
    private final List<OsmRelationPreprocessor> osmRelationPreprocessors = new ArrayList<>();
    /** Handlers that get a callback when each source is finished reading. */
    private final List<FinishHandler> finishHandlers = new ArrayList<>();
    /** Map from layer name to its handler if it implements {@link FeaturePostProcessor}. */
    private final Map<String, List<FeaturePostProcessor>> postProcessors = new HashMap<>();
    /** Map from source ID to its handler if it implements {@link FeatureProcessor}. */
    private final Map<String, List<FeatureProcessor>> sourceElementProcessors = new HashMap<>();


    protected void registerSourceHandler(String source, FeatureProcessor processor) {
      sourceElementProcessors.computeIfAbsent(source, name -> new ArrayList<>())
          .add(processor);
    }

    protected void registerHandler(Handler handler) {
      this.handlers.add(handler);
      if (handler instanceof OsmRelationPreprocessor osmRelationPreprocessor) {
        osmRelationPreprocessors.add(osmRelationPreprocessor);
      }
      if (handler instanceof FinishHandler finishHandler) {
        finishHandlers.add(finishHandler);
      }
      if (handler instanceof FeaturePostProcessor postProcessor) {
        postProcessors.computeIfAbsent(postProcessor.name(), name -> new ArrayList<>())
            .add(postProcessor);
      }
    }

    @Override
    public List<OsmRelationInfo> preprocessOsmRelation(OsmElement.Relation relation) {
      // delegate OSM relation pre-processing to each layer, if it implements FeaturePostProcessor
      List<OsmRelationInfo> result = null;
      for (OsmRelationPreprocessor osmRelationPreprocessor : osmRelationPreprocessors) {
        List<OsmRelationInfo> thisResult = osmRelationPreprocessor
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
      // delegate source feature processing to each handler for that source
      var handlers = sourceElementProcessors.get(sourceFeature.getSource());
      if (handlers != null) {
        for (var handler : handlers) {
          handler.processFeature(sourceFeature, features);
          // TODO extract common handling for expression-based filtering from openmaptiles to this
          // common profile when we have another use-case for it.
        }
      }
    }

    @Override
    public boolean caresAboutSource(String name) {
      return sourceElementProcessors.containsKey(name);
    }

    @Override
    public List<VectorTile.Feature> postProcessLayerFeatures(String layer, int zoom, List<VectorTile.Feature> items)
        throws GeometryException {
      // delegate feature post-processing to each layer, if it implements FeaturePostProcessor
      List<FeaturePostProcessor> handlers = postProcessors.get(layer);
      List<VectorTile.Feature> result = null;
      if (handlers != null) {
        for (FeaturePostProcessor handler : handlers) {
          var thisResult = handler.postProcess(zoom, items);
          if (thisResult != null) {
            if (result == null) {
              result = thisResult;
            } else {
              result.addAll(thisResult);
            }
          }
        }
      }
      return result == null ? items : result;
    }

    @Override
    public void finish(String sourceName, FeatureCollector.Factory featureCollectors,
        Consumer<FeatureCollector.Feature> next) {
      // delegate finish handling to every layer that implements FinishHandler
      for (var handler : finishHandlers) {
        handler.finish(sourceName, featureCollectors, next);
      }
    }

    @Override
    public void release() {
      // release resources used by each handler
      handlers.forEach(Handler::release);
    }

    /** Interface for handlers that this profile forwards to should implement. */
    public interface Handler {

      /** Free any resources associated with this profile (i.e. shared data structures) */
      default void release() {
      }
    }

    public interface HandlerForLayer {

      /** The layer name this handler is for */
      String name();
    }

    /** Handlers should implement this interface to get notified when a source finishes processing. */
    public interface FinishHandler {

      /**
       * Invoked once for each source after all elements for a source have been processed.
       *
       * @see Profile#finish(String, FeatureCollector.Factory, Consumer)
       */
      void finish(String sourceName, FeatureCollector.Factory featureCollectors,
          Consumer<FeatureCollector.Feature> emit);
    }

    /** Handlers should implement this interface to pre-process OSM relations during pass 1 through the data. */
    public interface OsmRelationPreprocessor {

      /**
       * Returns information extracted from an OSM relation during pass 1 of the input OSM data to make available when
       * processing elements in that relation during pass 2.
       *
       * @see Profile#preprocessOsmRelation(OsmElement.Relation)
       */
      List<OsmRelationInfo> preprocessOsmRelation(OsmElement.Relation relation);
    }

    /** Handlers should implement this interface to post-process vector tile features before emitting an output tile. */
    public interface FeaturePostProcessor extends HandlerForLayer {

      /**
       * Apply any post-processing to features in this output layer of a tile before writing it to the output file.
       *
       * @throws GeometryException if the input elements cannot be deserialized, or output elements cannot be
       *                           serialized
       * @see Profile#postProcessLayerFeatures(String, int, List)
       */
      List<VectorTile.Feature> postProcess(int zoom, List<VectorTile.Feature> items) throws GeometryException;
    }

    /** Handlers should implement this interface to process input features from a given source ID. */
    public interface FeatureProcessor {

      /**
       * Process an input element from a source feature.
       *
       * @see Profile#processFeature(SourceFeature, FeatureCollector)
       */
      void processFeature(SourceFeature feature, FeatureCollector features);
    }
  }
}
