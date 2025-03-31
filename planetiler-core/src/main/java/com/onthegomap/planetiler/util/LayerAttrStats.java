package com.onthegomap.planetiler.util;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.onthegomap.planetiler.archive.WriteableTileArchive;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import net.jcip.annotations.ThreadSafe;

/**
 * Tracks the feature attributes and zoom range of each layer to populate the archive output metadata.
 * <p>
 * Matches the MBTiles spec for {@code vector_layers}, but can be reused by other {@link WriteableTileArchive} formats.
 * To minimize overhead of stat collection, each updating thread should call {@link #handlerForThread()} first to get a
 * thread-local handler that can update stats without contention.
 * </p>
 *
 * @see com.onthegomap.planetiler.archive.TileArchiveMetadata.TileArchiveMetadataJson
 * @see <a href="https://github.com/mapbox/mbtiles-spec/blob/master/1.3/spec.md#content">MBtiles spec</a>
 */
@ThreadSafe
public class LayerAttrStats {
  /*
   * This utility is called for billions of features by multiple threads when processing the planet which can make
   * access to shared data structures a bottleneck.  So give each thread an individual ThreadLocalLayerStatsHandler to
   * update and aggregate the results on read.
   */

  private final List<ThreadLocalHandler> threadLocals = new CopyOnWriteArrayList<>();
  // Ignore warnings about not removing thread local values since planetiler uses dedicated worker threads that release
  // values when a task is finished and are not re-used.
  @SuppressWarnings("java:S5164")
  private final ThreadLocal<ThreadLocalHandler> layerStats = ThreadLocal
    .withInitial(ThreadLocalHandler::new);

  /** Returns stats on all features that have been emitted as a list of {@link VectorLayer} objects. */
  public List<VectorLayer> getTileStats() {
    Map<String, StatsForLayer> layers = new TreeMap<>();
    for (var threadLocal : threadLocals) {
      for (StatsForLayer stats : threadLocal.layers.values()) {
        layers.merge(stats.layer, stats, (prev, next) -> {
          prev.expandZoomRangeToInclude(next.maxzoom);
          prev.expandZoomRangeToInclude(next.minzoom);
          for (var entry : next.fields.entrySet()) {
            // keep track of field type as a number/boolean/string but widen to string if multiple different
            // types are encountered
            prev.fields.merge(entry.getKey(), entry.getValue(), FieldType::merge);
          }
          return prev;
        });
      }
    }
    return layers.values().stream()
      .map(stats -> new VectorLayer(stats.layer, stats.fields, stats.minzoom, stats.maxzoom))
      .toList();
  }

  /** Shortcut for tests */
  void accept(String layer, int zoom, String key, Object value) {
    handlerForThread().forZoom(zoom).forLayer(layer).accept(key, value);
  }

  public enum FieldType {
    @JsonProperty("Number")
    NUMBER,
    @JsonProperty("Boolean")
    BOOLEAN,
    @JsonProperty("String")
    STRING;

    /**
     * Per the MBTiles spec: attributes whose type varies between features SHOULD be listed as "String"
     */
    public static FieldType merge(FieldType oldValue, FieldType newValue) {
      return oldValue != newValue ? STRING : newValue;
    }
  }

  public record VectorLayer(
    @JsonProperty("id") String id,
    @JsonProperty("fields") Map<String, FieldType> fields,
    @JsonProperty("description") Optional<String> description,
    @JsonProperty("minzoom") OptionalInt minzoom,
    @JsonProperty("maxzoom") OptionalInt maxzoom
  ) {

    public VectorLayer(String id, Map<String, FieldType> fields) {
      this(id, fields, Optional.empty(), OptionalInt.empty(), OptionalInt.empty());
    }

    public VectorLayer(String id, Map<String, FieldType> fields, int minzoom, int maxzoom) {
      this(id, fields, Optional.empty(), OptionalInt.of(minzoom), OptionalInt.of(maxzoom));
    }

    public VectorLayer withDescription(String newDescription) {
      return new VectorLayer(id, fields, Optional.of(newDescription), minzoom, maxzoom);
    }

    public VectorLayer withMinzoom(int newMinzoom) {
      return new VectorLayer(id, fields, description, OptionalInt.of(newMinzoom), maxzoom);
    }

    public VectorLayer withMaxzoom(int newMaxzoom) {
      return new VectorLayer(id, fields, description, minzoom, OptionalInt.of(newMaxzoom));
    }
  }

  /** Accepts features from a single thread that will be combined across all threads in {@link #getTileStats()}. */
  @ThreadSafe
  private class ThreadLocalHandler implements Updater {

    private final Map<String, StatsForLayer> layers = new TreeMap<>();

    ThreadLocalHandler() {
      threadLocals.add(this);
    }

    @Override
    public Updater.ForZoom forZoom(int zoom) {
      return layer -> {
        var stats = layers.computeIfAbsent(layer, StatsForLayer::new);
        stats.expandZoomRangeToInclude(zoom);
        return (key, value) -> {
          FieldType fieldType = null;
          if (value instanceof Number) {
            fieldType = FieldType.NUMBER;
          } else if (value instanceof Boolean) {
            fieldType = FieldType.BOOLEAN;
          } else if (value != null) {
            fieldType = FieldType.STRING;
          }
          if (fieldType != null) {
            // widen different types to string
            stats.fields.merge(key, fieldType, FieldType::merge);
          }
        };
      };
    }
  }

  /**
   * Returns a handler optimized for accepting features from a single thread.
   */
  public Updater handlerForThread() {
    return layerStats.get();
  }

  public interface Updater {

    ForZoom forZoom(int zoom);

    interface ForZoom {

      ForZoom NOOP = layer -> (key, value) -> {
      };

      ForLayer forLayer(String layer);

      interface ForLayer {
        void accept(String key, Object value);
      }
    }
  }

  private static class StatsForLayer {

    private final String layer;
    // use TreeMap to ensure the same output always appears the same in an archive
    private final Map<String, FieldType> fields = new TreeMap<>();
    private int minzoom = Integer.MAX_VALUE;
    private int maxzoom = Integer.MIN_VALUE;

    private StatsForLayer(String layer) {
      this.layer = layer;
    }

    private void expandZoomRangeToInclude(int zoom) {
      minzoom = Math.min(zoom, minzoom);
      maxzoom = Math.max(zoom, maxzoom);
    }
  }
}
