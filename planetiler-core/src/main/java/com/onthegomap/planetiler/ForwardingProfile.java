package com.onthegomap.planetiler;

import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.reader.osm.OsmElement;
import com.onthegomap.planetiler.reader.osm.OsmRelationInfo;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * A framework for building complex {@link Profile Profiles} that need to be broken apart into multiple handlers (i.e.
 * one per layer).
 * <p>
 * Individual handlers added with {@link #registerHandler(Handler)} can listen on events by implementing these handlers:
 * <ul>
 * <li>{@link OsmRelationPreprocessor} to process every OSM relation during first pass through OSM file</li>
 * <li>{@link FeatureProcessor} to handle features from a particular source (added through
 * {@link #registerSourceHandler(String, FeatureProcessor)})</li>
 * <li>{@link FinishHandler} to be notified whenever we finish processing each source</li>
 * <li>{@link FeaturePostProcessor} to post-process features in a layer before rendering the output tile</li>
 * </ul>
 * See {@code BasemapProfile} for a full implementation using this framework.
 */
public abstract class ForwardingProfile implements Profile {

  private final List<Handler> handlers = new ArrayList<>();
  /** Handlers that pre-process OSM nodes during pass 1 through the data. */
  private final List<OsmNodePreprocessor> osmNodePreprocessors = new ArrayList<>();
  /** Handlers that pre-process OSM ways during pass 1 through the data. */
  private final List<OsmWayPreprocessor> osmWayPreprocessors = new ArrayList<>();
  /** Handlers that pre-process OSM relations during pass 1 through the data. */
  private final List<OsmRelationPreprocessor> osmRelationPreprocessors = new ArrayList<>();
  /** Handlers that get a callback when each source is finished reading. */
  private final List<FinishHandler> finishHandlers = new ArrayList<>();
  /** Map from layer name to its handler if it implements {@link FeaturePostProcessor}. */
  private final Map<String, List<FeaturePostProcessor>> postProcessors = new HashMap<>();
  /** Map from source ID to its handler if it implements {@link FeatureProcessor}. */
  private final Map<String, List<FeatureProcessor>> sourceElementProcessors = new HashMap<>();

  /**
   * Call {@code processor} for every element in {@code source}.
   *
   * @param source    string ID of the source
   * @param processor handler that will process elements in that source
   */
  public void registerSourceHandler(String source, FeatureProcessor processor) {
    sourceElementProcessors.computeIfAbsent(source, name -> new ArrayList<>())
      .add(processor);
  }

  /**
   * Call {@code handler} for different events based on which interfaces {@code handler} implements:
   * {@link OsmRelationPreprocessor}, {@link FinishHandler}, or {@link FeaturePostProcessor}.
   */
  public void registerHandler(Handler handler) {
    this.handlers.add(handler);
    if (handler instanceof OsmNodePreprocessor osmNodePreprocessor) {
      osmNodePreprocessors.add(osmNodePreprocessor);
    }
    if (handler instanceof OsmWayPreprocessor osmWayPreprocessor) {
      osmWayPreprocessors.add(osmWayPreprocessor);
    }
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
  public void preprocessOsmNode(OsmElement.Node node) {
    for (OsmNodePreprocessor osmNodePreprocessor : osmNodePreprocessors) {
      osmNodePreprocessor.preprocessOsmNode(node);
    }
  }

  @Override
  public void preprocessOsmWay(OsmElement.Way way) {
    for (OsmWayPreprocessor osmWayPreprocessor : osmWayPreprocessors) {
      osmWayPreprocessor.preprocessOsmWay(way);
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
        // TODO extract common handling for expression-based filtering from basemap to this
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
    List<VectorTile.Feature> result = items;
    if (handlers != null) {
      for (FeaturePostProcessor handler : handlers) {
        var thisResult = handler.postProcess(zoom, result);
        if (thisResult != null) {
          result = thisResult;
        }
      }
    }
    return result;
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
    default void release() {}
  }

  public interface HandlerForLayer extends Handler {

    /** The layer name this handler is for */
    String name();
  }

  /** Handlers should implement this interface to get notified when a source finishes processing. */
  public interface FinishHandler extends Handler {

    /**
     * Invoked once for each source after all elements for a source have been processed.
     *
     * @see Profile#finish(String, FeatureCollector.Factory, Consumer)
     */
    void finish(String sourceName, FeatureCollector.Factory featureCollectors,
      Consumer<FeatureCollector.Feature> emit);
  }

  /** Handlers should implement this interface to pre-process OSM nodes during pass 1 through the data. */
  public interface OsmNodePreprocessor extends Handler {

    /**
     * Extracts information from an OSM node during pass 1 of the input OSM data that the profile may need during pass2.
     *
     * @see Profile#preprocessOsmNode(OsmElement.Node)
     */
    void preprocessOsmNode(OsmElement.Node node);
  }


  /** Handlers should implement this interface to pre-process OSM ways during pass 1 through the data. */
  public interface OsmWayPreprocessor extends Handler {

    /**
     * Extracts information from an OSM way during pass 1 of the input OSM data that the profile may need during pass2.
     *
     * @see Profile#preprocessOsmWay(OsmElement.Way)
     */
    void preprocessOsmWay(OsmElement.Way way);
  }

  /** Handlers should implement this interface to pre-process OSM relations during pass 1 through the data. */
  public interface OsmRelationPreprocessor extends Handler {

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
     * @throws GeometryException if the input elements cannot be deserialized, or output elements cannot be serialized
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
