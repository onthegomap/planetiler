package com.onthegomap.flatmap.util;

import com.onthegomap.flatmap.mbiles.Mbtiles;
import com.onthegomap.flatmap.render.RenderedFeature;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

public class LayerStats implements Consumer<RenderedFeature> {

  private final List<ThreadLocalLayerStatsHandler> threadLocals = new CopyOnWriteArrayList<>();
  private final ThreadLocal<ThreadLocalLayerStatsHandler> layerStats = ThreadLocal
    .withInitial(ThreadLocalLayerStatsHandler::new);

  public Mbtiles.MetadataJson getTileStats() {
    Map<String, VectorTileStats> layers = new TreeMap<>();
    for (var threadLocal : threadLocals) {
      for (VectorTileStats stats : threadLocal.layers.values()) {
        layers.merge(stats.layer, stats, (oldOne, newOne) -> {
          oldOne.zoom(newOne.maxzoom);
          oldOne.zoom(newOne.minzoom);
          for (var entry : newOne.fields.entrySet()) {
            oldOne.fields.merge(entry.getKey(), entry.getValue(), Mbtiles.MetadataJson.FieldType::merge);
          }
          return oldOne;
        });
      }
    }
    return new Mbtiles.MetadataJson(
      layers.values().stream()
        .map(stats -> new Mbtiles.MetadataJson.VectorLayer(stats.layer, stats.fields, stats.minzoom, stats.maxzoom))
        .toList()
    );
  }

  private class ThreadLocalLayerStatsHandler implements Consumer<RenderedFeature> {

    private final Map<String, VectorTileStats> layers = new TreeMap<>();

    ThreadLocalLayerStatsHandler() {
      threadLocals.add(this);
    }

    @Override
    public void accept(RenderedFeature feature) {
      int zoom = feature.tile().z();
      var vectorTileFeature = feature.vectorTileFeature();
      var stats = layers.computeIfAbsent(vectorTileFeature.layer(), VectorTileStats::new);
      stats.zoom(zoom);
      for (Map.Entry<String, Object> entry : vectorTileFeature.attrs().entrySet()) {
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
          stats.fields.merge(key, fieldType, Mbtiles.MetadataJson.FieldType::merge);
        }
      }
    }
  }

  public Consumer<RenderedFeature> handlerForThread() {
    return layerStats.get();
  }

  @Override
  public void accept(RenderedFeature feature) {
    handlerForThread().accept(feature);
  }

  private static class VectorTileStats {

    private final String layer;
    private final Map<String, Mbtiles.MetadataJson.FieldType> fields = new HashMap<>();
    private int minzoom = Integer.MAX_VALUE;
    private int maxzoom = Integer.MIN_VALUE;

    private VectorTileStats(String layer) {
      this.layer = layer;
    }

    private void zoom(int zoom) {
      minzoom = Math.min(zoom, minzoom);
      maxzoom = Math.max(zoom, maxzoom);
    }
  }
}
