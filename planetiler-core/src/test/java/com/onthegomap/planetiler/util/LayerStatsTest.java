package com.onthegomap.planetiler.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.onthegomap.planetiler.VectorTile;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.mbtiles.Mbtiles;
import com.onthegomap.planetiler.render.RenderedFeature;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

public class LayerStatsTest {

  final LayerStats layerStats = new LayerStats();

  @Test
  public void testEmptyLayerStats() {
    assertEquals(new Mbtiles.MetadataJson(), layerStats.getTileStats());
  }

  @Test
  public void testEmptyLayerStatsOneLayer() {
    layerStats.accept(new RenderedFeature(
      TileCoord.ofXYZ(1, 2, 3),
      new VectorTile.Feature(
        "layer1",
        1,
        VectorTile.encodeGeometry(GeoUtils.point(1, 2)),
        Map.of("a", 1, "b", "string", "c", true)
      ),
      1,
      Optional.empty()
    ));
    assertEquals(new Mbtiles.MetadataJson(
      new Mbtiles.MetadataJson.VectorLayer("layer1", Map.of(
        "a", Mbtiles.MetadataJson.FieldType.NUMBER,
        "b", Mbtiles.MetadataJson.FieldType.STRING,
        "c", Mbtiles.MetadataJson.FieldType.BOOLEAN
      ), 3, 3)
    ), layerStats.getTileStats());
  }

  @Test
  public void testEmptyLayerStatsTwoLayers() {
    layerStats.accept(new RenderedFeature(
      TileCoord.ofXYZ(1, 2, 3),
      new VectorTile.Feature(
        "layer1",
        1,
        VectorTile.encodeGeometry(GeoUtils.point(1, 2)),
        Map.of()
      ),
      1,
      Optional.empty()
    ));
    layerStats.accept(new RenderedFeature(
      TileCoord.ofXYZ(1, 2, 4),
      new VectorTile.Feature(
        "layer2",
        1,
        VectorTile.encodeGeometry(GeoUtils.point(1, 2)),
        Map.of("a", 1, "b", true, "c", true)
      ),
      1,
      Optional.empty()
    ));
    layerStats.accept(new RenderedFeature(
      TileCoord.ofXYZ(1, 2, 1),
      new VectorTile.Feature(
        "layer2",
        1,
        VectorTile.encodeGeometry(GeoUtils.point(1, 2)),
        Map.of("a", 1, "b", true, "c", 1)
      ),
      1,
      Optional.empty()
    ));
    assertEquals(new Mbtiles.MetadataJson(
      new Mbtiles.MetadataJson.VectorLayer("layer1", Map.of(
      ), 3, 3),
      new Mbtiles.MetadataJson.VectorLayer("layer2", Map.of(
        "a", Mbtiles.MetadataJson.FieldType.NUMBER,
        "b", Mbtiles.MetadataJson.FieldType.BOOLEAN,
        "c", Mbtiles.MetadataJson.FieldType.STRING
      ), 1, 4)
    ), layerStats.getTileStats());
  }

  @Test
  public void testMergeFromMultipleThreads() throws InterruptedException {
    Thread t1 = new Thread(() -> layerStats.accept(new RenderedFeature(
      TileCoord.ofXYZ(1, 2, 3),
      new VectorTile.Feature(
        "layer1",
        1,
        VectorTile.encodeGeometry(GeoUtils.point(1, 2)),
        Map.of("a", 1)
      ),
      1,
      Optional.empty()
    )));
    t1.start();
    Thread t2 = new Thread(() -> layerStats.accept(new RenderedFeature(
      TileCoord.ofXYZ(1, 2, 4),
      new VectorTile.Feature(
        "layer1",
        1,
        VectorTile.encodeGeometry(GeoUtils.point(1, 2)),
        Map.of("a", true)
      ),
      1,
      Optional.empty()
    )));
    t2.start();
    t1.join();
    t2.join();
    assertEquals(new Mbtiles.MetadataJson(
      new Mbtiles.MetadataJson.VectorLayer("layer1", Map.of(
        "a", Mbtiles.MetadataJson.FieldType.STRING
      ), 3, 4)
    ), layerStats.getTileStats());
  }
}
