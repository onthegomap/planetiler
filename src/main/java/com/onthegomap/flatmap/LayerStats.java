package com.onthegomap.flatmap;

import com.onthegomap.flatmap.write.Mbtiles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Consumer;

public class LayerStats implements Consumer<RenderedFeature> {

  private final List<Map<String, VectorTileStats>> threadLocals = Collections.synchronizedList(new ArrayList<>());
  private final ThreadLocal<Map<String, VectorTileStats>> layerStats = ThreadLocal.withInitial(() -> {
    Map<String, VectorTileStats> result = new TreeMap<>();
    threadLocals.add(result);
    return result;
  });

  private static VectorTileStats merge(VectorTileStats a, VectorTileStats b) {
    for (var entry : b.fields.entrySet()) {
      a.fields.merge(entry.getKey(), entry.getValue(), Mbtiles.MetadataJson.FieldType::merge);
      a.zoom(b.minzoom);
      a.zoom(b.maxzoom);
    }
    return a;
  }

  public Mbtiles.MetadataJson getTileStats() {
    synchronized (threadLocals) {
      Map<String, VectorTileStats> layers = new TreeMap<>();
      for (Map<String, VectorTileStats> threadLocal : threadLocals) {
        for (VectorTileStats stats : threadLocal.values()) {
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
  }

  @Override
  public void accept(RenderedFeature feature) {
    int zoom = feature.tile().z();
    var vectorTileFeature = feature.vectorTileFeature();
    var layers = layerStats.get();
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
