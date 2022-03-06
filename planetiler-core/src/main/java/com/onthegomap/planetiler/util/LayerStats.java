package com.onthegomap.planetiler.util;

import com.onthegomap.planetiler.mbtiles.Mbtiles;
import com.onthegomap.planetiler.render.RenderedFeature;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Tracks the type and zoom range of vector tile attributes that have been emitted by layer to
 * populate the {@code json} attribute in the mbtiles output metadata.
 *
 * <p>To minimize overhead of stat collection, each updating thread should call {@link
 * #handlerForThread()} first to get a thread-local handler that can update stats without
 * contention.
 *
 * @see Mbtiles.MetadataJson
 * @see <a href="https://github.com/mapbox/mbtiles-spec/blob/master/1.3/spec.md#content">MBtiles
 *     spec</a>
 */
@ThreadSafe
public class LayerStats implements Consumer<RenderedFeature> {
  /*
   * This utility is called for billions of features by multiple threads when processing the planet which can make
   * access to shared data structures a bottleneck.  So give each thread an individual ThreadLocalLayerStatsHandler to
   * update and aggregate the results on read.
   */

  private final List<ThreadLocalHandler> threadLocals = new CopyOnWriteArrayList<>();
  private final ThreadLocal<ThreadLocalHandler> layerStats =
      ThreadLocal.withInitial(ThreadLocalHandler::new);

  /**
   * Returns stats on all features that have been emitted for the {@code json} mbtiles metadata
   * value.
   */
  public Mbtiles.MetadataJson getTileStats() {
    Map<String, StatsForLayer> layers = new TreeMap<>();
    for (var threadLocal : threadLocals) {
      for (StatsForLayer stats : threadLocal.layers.values()) {
        layers.merge(
            stats.layer,
            stats,
            (prev, next) -> {
              prev.expandZoomRangeToInclude(next.maxzoom);
              prev.expandZoomRangeToInclude(next.minzoom);
              for (var entry : next.fields.entrySet()) {
                // keep track of field type as a number/boolean/string but widen to string if
                // multiple different
                // types are encountered
                prev.fields.merge(
                    entry.getKey(), entry.getValue(), Mbtiles.MetadataJson.FieldType::merge);
              }
              return prev;
            });
      }
    }
    return new Mbtiles.MetadataJson(
        layers.values().stream()
            .map(
                stats ->
                    new Mbtiles.MetadataJson.VectorLayer(
                        stats.layer, stats.fields, stats.minzoom, stats.maxzoom))
            .toList());
  }

  /**
   * Accepts features from a single thread that will be combined across all threads in {@link
   * #getTileStats()}.
   */
  @NotThreadSafe
  private class ThreadLocalHandler implements Consumer<RenderedFeature> {

    private final Map<String, StatsForLayer> layers = new TreeMap<>();

    ThreadLocalHandler() {
      threadLocals.add(this);
    }

    @Override
    public void accept(RenderedFeature feature) {
      var vectorTileFeature = feature.vectorTileFeature();
      var stats = layers.computeIfAbsent(vectorTileFeature.layer(), StatsForLayer::new);
      stats.expandZoomRangeToInclude(feature.tile().z());
      for (var entry : vectorTileFeature.attrs().entrySet()) {
        String key = entry.getKey();
        Object value = entry.getValue();

        Mbtiles.MetadataJson.FieldType fieldType = null;
        if (value instanceof Number) {
          fieldType = Mbtiles.MetadataJson.FieldType.NUMBER;
        } else if (value instanceof Boolean) {
          fieldType = Mbtiles.MetadataJson.FieldType.BOOLEAN;
        } else if (value != null) {
          fieldType = Mbtiles.MetadataJson.FieldType.STRING;
        }
        if (fieldType != null) {
          // widen different types to string
          stats.fields.merge(key, fieldType, Mbtiles.MetadataJson.FieldType::merge);
        }
      }
    }
  }

  /**
   * Returns a handler optimized for accepting features from a single thread.
   *
   * <p>Use this instead of {@link #accept(RenderedFeature)}
   */
  public Consumer<RenderedFeature> handlerForThread() {
    return layerStats.get();
  }

  @Override
  public void accept(RenderedFeature feature) {
    handlerForThread().accept(feature);
  }

  private static class StatsForLayer {

    private final String layer;
    private final Map<String, Mbtiles.MetadataJson.FieldType> fields = new HashMap<>();
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
