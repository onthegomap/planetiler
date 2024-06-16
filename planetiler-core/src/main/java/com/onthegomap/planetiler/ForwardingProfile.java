package com.onthegomap.planetiler;

import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.expression.Expression;
import com.onthegomap.planetiler.expression.MultiExpression;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.reader.osm.OsmElement;
import com.onthegomap.planetiler.reader.osm.OsmRelationInfo;
import java.util.AbstractSequentialList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SequencedMap;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
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
 * <li>{@link LayerPostProcesser} to post-process features in a layer before rendering the output tile</li>
 * <li>{@link TilePostProcessor} to post-process features in a tile before rendering the output tile</li>
 * </ul>
 * See {@code OpenMapTilesProfile} for a full implementation using this framework.
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
  /** Map from layer name to its handler if it implements {@link LayerPostProcesser}. */
  private final Map<String, List<LayerPostProcesser>> layerPostProcessors = new HashMap<>();
  /** List of handlers that implement {@link TilePostProcessor}. */
  private final List<TilePostProcessor> tilePostProcessors = new ArrayList<>();
  /** List of handlers that implements {@link FeatureProcessor} along with a filter expression. */
  private final List<MultiExpression.Entry<FeatureProcessor>> sourceElementProcessors = new CopyOnWriteArrayList<>();
  private final List<String> onlyLayers;
  private final List<String> excludeLayers;
  @SuppressWarnings("java:S3077")
  private volatile MultiExpression.Index<FeatureProcessor> indexedSourceElementProcessors = null;

  protected ForwardingProfile(PlanetilerConfig config, Handler... handlers) {
    onlyLayers = config.arguments().getList("only_layers", "Include only certain layers", List.of());
    excludeLayers = config.arguments().getList("exclude_layers", "Exclude certain layers", List.of());
    for (var handler : handlers) {
      registerHandler(handler);
    }
  }

  protected ForwardingProfile() {
    onlyLayers = List.of();
    excludeLayers = List.of();
  }

  protected ForwardingProfile(Handler... handlers) {
    onlyLayers = List.of();
    excludeLayers = List.of();
    for (var handler : handlers) {
      registerHandler(handler);
    }
  }

  private boolean caresAboutLayer(String layer) {
    return (onlyLayers.isEmpty() || onlyLayers.contains(layer)) && !excludeLayers.contains(layer);
  }

  private boolean caresAboutLayer(Object obj) {
    return !(obj instanceof HandlerForLayer l) || caresAboutLayer(l.name());
  }

  /**
   * Call {@code processor} for every element in {@code source}.
   *
   * @param source    string ID of the source
   * @param processor handler that will process elements in that source
   */
  public void registerSourceHandler(String source, FeatureProcessor processor) {
    if (!caresAboutLayer(processor)) {
      return;
    }
    sourceElementProcessors
      .add(MultiExpression.entry(processor, Expression.and(Expression.matchSource(source), processor.filter())));
    synchronized (sourceElementProcessors) {
      indexedSourceElementProcessors = null;
    }
  }

  /**
   * Call {@code processor} for every element.
   *
   * @param processor handler that will process elements in that source
   */
  public void registerFeatureHandler(FeatureProcessor processor) {
    if (!caresAboutLayer(processor)) {
      return;
    }
    sourceElementProcessors.add(MultiExpression.entry(processor, processor.filter()));
    synchronized (sourceElementProcessors) {
      indexedSourceElementProcessors = null;
    }
  }

  /**
   * Call {@code handler} for different events based on which interfaces {@code handler} implements:
   * {@link OsmRelationPreprocessor}, {@link FinishHandler}, {@link TilePostProcessor} or {@link LayerPostProcesser}.
   */
  public void registerHandler(Handler handler) {
    if (!caresAboutLayer(handler)) {
      return;
    }
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
    if (handler instanceof LayerPostProcesser postProcessor) {
      layerPostProcessors.computeIfAbsent(postProcessor.name(), name -> new ArrayList<>())
        .add(postProcessor);
    }
    if (handler instanceof TilePostProcessor postProcessor) {
      tilePostProcessors.add(postProcessor);
    }
    if (handler instanceof FeatureProcessor processor) {
      registerFeatureHandler(processor);
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
    for (var handler : getHandlerIndex().getMatches(sourceFeature)) {
      handler.processFeature(sourceFeature, features);
    }
  }

  private MultiExpression.Index<FeatureProcessor> getHandlerIndex() {
    MultiExpression.Index<FeatureProcessor> result = indexedSourceElementProcessors;
    if (result == null) {
      synchronized (sourceElementProcessors) {
        result = indexedSourceElementProcessors;
        if (result == null) {
          indexedSourceElementProcessors = result = MultiExpression.of(sourceElementProcessors).index();
        }
      }
    }
    return result;
  }

  @Override
  public boolean caresAboutSource(String name) {
    return caresAbout(Expression.PartialInput.ofSource(name));
  }

  @Override
  public boolean caresAbout(Expression.PartialInput input) {
    return sourceElementProcessors.stream().anyMatch(e -> e.expression()
      .partialEvaluate(input)
      .simplify() != Expression.FALSE);
  }

  @Override
  public List<VectorTile.Feature> postProcessLayerFeatures(String layer, int zoom, List<VectorTile.Feature> items)
    throws GeometryException {
    // delegate feature post-processing to each layer, if it implements FeaturePostProcessor
    List<LayerPostProcesser> postProcessers = layerPostProcessors.get(layer);
    List<VectorTile.Feature> result = makeMutable(items);
    if (postProcessers != null) {
      for (var handler : postProcessers) {
        var thisResult = handler.postProcess(zoom, result);
        if (thisResult != null && result != thisResult) {
          result = makeMutable(thisResult);
        }
      }
    }
    return result;
  }

  @Override
  public Map<String, List<VectorTile.Feature>> postProcessTileFeatures(TileCoord tileCoord,
    Map<String, List<VectorTile.Feature>> layers) throws GeometryException {
    var result = makeMutable(layers);
    for (TilePostProcessor postProcessor : tilePostProcessors) {
      // TODO catch failures to isolate from other tile postprocessors?
      var thisResult = postProcessor.postProcessTile(tileCoord, result);
      if (thisResult != null && result != thisResult) {
        result = makeMutable(thisResult);
      }
    }
    return result;
  }

  private <T> List<T> makeMutable(List<T> list) {
    return switch (list) {
      case ArrayList<T> l -> list;
      case LinkedList<T> l -> list;
      case AbstractSequentialList<T> l -> new LinkedList<>(list);
      case null -> null;
      default -> new ArrayList<>(list);
    };
  }

  private <K, V> Map<K, List<V>> makeMutable(Map<K, List<V>> map) {
    var mutableMap = makeMutableMap(map);
    if (mutableMap != null) {
      for (var entry : map.entrySet()) {
        var key = entry.getKey();
        var value = entry.getValue();
        var mutableList = makeMutable(value);
        if (mutableList != value) {
          mutableMap.put(key, mutableList);
        }
      }
    }
    return mutableMap;
  }

  private <K, V> Map<K, V> makeMutableMap(Map<K, V> map) {
    return switch (map) {
      case HashMap<K, V> m -> map;
      case TreeMap<K, V> m -> map;
      case NavigableMap<K, V> m -> new TreeMap<>(m);
      case SequencedMap<K, V> m -> new LinkedHashMap<>(map);
      case null -> null;
      default -> new HashMap<>(map);
    };
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

  /** Handlers should implement this interface to post-process vector tile features before emitting an output layer. */
  public interface LayerPostProcesser extends HandlerForLayer {

    /**
     * Apply any post-processing to features in this output layer of a tile before writing it to the output archive.
     *
     * @throws GeometryException if the input elements cannot be deserialized, or output elements cannot be serialized
     * @see Profile#postProcessLayerFeatures(String, int, List)
     */
    List<VectorTile.Feature> postProcess(int zoom, List<VectorTile.Feature> items) throws GeometryException;
  }

  /** @deprecated use {@link LayerPostProcesser} or {@link TilePostProcessor} instead */
  @Deprecated(forRemoval = true)
  public interface FeaturePostProcessor extends LayerPostProcesser {}

  /**
   * Handlers should implement this interface to post-process all features in a vector tile before writing to an
   * archive.
   */
  public interface TilePostProcessor extends Handler {

    /**
     * Apply any post-processing to features in layers in this output tile before writing it to the output archive.
     *
     * @throws GeometryException if the input elements cannot be deserialized, or output elements cannot be serialized
     * @see Profile#postProcessTileFeatures(TileCoord, Map)
     */
    Map<String, List<VectorTile.Feature>> postProcessTile(TileCoord tileCoord,
      Map<String, List<VectorTile.Feature>> layers) throws GeometryException;
  }

  /** Handlers should implement this interface to process input features from a given source ID. */
  @FunctionalInterface
  public interface FeatureProcessor extends com.onthegomap.planetiler.FeatureProcessor<SourceFeature>, Handler {

    /** Returns an {@link Expression} that limits the features that this processor gets called for. */
    default Expression filter() {
      return Expression.TRUE;
    }
  }
}
