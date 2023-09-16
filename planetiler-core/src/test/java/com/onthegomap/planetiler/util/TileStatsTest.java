package com.onthegomap.planetiler.util;

import static com.onthegomap.planetiler.TestUtils.newPoint;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.onthegomap.planetiler.VectorTile;
import com.onthegomap.planetiler.geo.TileCoord;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class TileStatsTest {
  @Test
  void computeStatsEmpty() {
    var stats = TileStats.computeTileStats(new VectorTile().toProto());
    assertEquals(0, stats.size());
  }

  @Test
  void computeStatsOneFeature() throws IOException {
    var stats = TileStats.computeTileStats(new VectorTile()
      .addLayerFeatures("layer", List.of(new VectorTile.Feature(
        "layer",
        1,
        VectorTile.encodeGeometry(newPoint(0, 0)),
        Map.of("key1", "value1", "key2", 2)
      )))
      .toProto());
    assertEquals(1, stats.size());
    var entry1 = stats.get(0);
    assertEquals("layer", entry1.layer());
    assertEquals(1, entry1.layerFeatures());
    assertEquals(55, entry1.layerBytes());

    assertEquals(18, entry1.layerAttrBytes());
    assertEquals(2, entry1.layerAttrKeys());
    assertEquals(2, entry1.layerAttrValues());

    var formatted = TileStats.formatOutputRows(TileCoord.ofXYZ(1, 2, 3), 999, stats);
    assertEquals(
      """
        z	x	y	hilbert	archived_tile_bytes	layer	layer_bytes	layer_features	layer_attr_bytes	layer_attr_keys	layer_attr_values
        3	1	2	34	999	layer	55	1	18	2	2
        """
        .trim(),
      (TileStats.headerRow() + String.join("", formatted)).trim());
  }

  @Test
  void computeStats2Features() throws IOException {
    var stats = TileStats.computeTileStats(new VectorTile()
      .addLayerFeatures("b", List.of(
        new VectorTile.Feature(
          "b",
          1,
          VectorTile.encodeGeometry(newPoint(0, 0)),
          Map.of()
        )
      ))
      .addLayerFeatures("a", List.of(
        new VectorTile.Feature(
          "a",
          1,
          VectorTile.encodeGeometry(newPoint(0, 0)),
          Map.of("key1", "value1", "key2", 2)
        ),
        new VectorTile.Feature(
          "a",
          2,
          VectorTile.encodeGeometry(newPoint(1, 1)),
          Map.of("key1", 2, "key2", 3)
        )
      ))
      .toProto());
    assertEquals(2, stats.size());
    var entry1 = stats.get(0);
    assertEquals("a", entry1.layer());
    assertEquals(2, entry1.layerFeatures());
    assertEquals(72, entry1.layerBytes());

    assertEquals(20, entry1.layerAttrBytes());
    assertEquals(2, entry1.layerAttrKeys());
    assertEquals(3, entry1.layerAttrValues());
    var entry2 = stats.get(1);
    assertEquals("b", entry2.layer());
    assertEquals(1, entry2.layerFeatures());

    var formatted = TileStats.formatOutputRows(TileCoord.ofXYZ(1, 2, 3), 999, stats);
    assertEquals(
      """
        z	x	y	hilbert	archived_tile_bytes	layer	layer_bytes	layer_features	layer_attr_bytes	layer_attr_keys	layer_attr_values
        3	1	2	34	999	a	72	2	20	2	3
        3	1	2	34	999	b	19	1	0	0	0
        """
        .trim(),
      (TileStats.headerRow() + String.join("", formatted)).trim());
  }

  @Test
  void aggregateTileStats() {
    var tileStats = new TileStats();
    var updater1 = tileStats.threadLocalUpdater();
    var updater2 = tileStats.threadLocalUpdater();
    updater1.recordTile(TileCoord.ofXYZ(0, 0, 1), 123, List.of(
      new TileStats.LayerStats("a", 1, 2, 3, 4, 5),
      new TileStats.LayerStats("b", 6, 7, 8, 9, 10)
    ));
    updater2.recordTile(TileCoord.ofXYZ(0, 1, 1), 345, List.of(
      new TileStats.LayerStats("b", 1, 2, 3, 4, 5),
      new TileStats.LayerStats("c", 6, 7, 8, 9, 10)
    ));
    var summary = tileStats.summary();
    assertEquals(Set.of("a", "b", "c"), Set.copyOf(summary.layers()));
    assertEquals(0, summary.get(0).maxSize());
    assertEquals(0, summary.get(0).numTiles());
    assertEquals(7, summary.get(1).maxSize());
    assertEquals(2, summary.get(1).numTiles());

    assertEquals(0, summary.get(0, "a").maxSize());
    assertEquals(1, summary.get(1, "a").maxSize());
    assertEquals(6, summary.get(1, "b").maxSize());
    assertEquals(6, summary.get(1, "c").maxSize());

    assertEquals(0, summary.get(0, "a").numTiles());
    assertEquals(1, summary.get(1, "a").numTiles());
    assertEquals(2, summary.get(1, "b").numTiles());
    assertEquals(1, summary.get(1, "c").numTiles());


    assertEquals(1, summary.get("a").maxSize());
    assertEquals(6, summary.get("b").maxSize());
    assertEquals(6, summary.get("c").maxSize());
    assertEquals(1, summary.get("a").numTiles());
    assertEquals(2, summary.get("b").numTiles());
    assertEquals(1, summary.get("c").numTiles());

    assertEquals(7, summary.get().maxSize());
    assertEquals(2, summary.get().numTiles());
  }

  @Test
  void topGzippedTiles() {
    var tileStats = new TileStats();
    var updater1 = tileStats.threadLocalUpdater();
    var updater2 = tileStats.threadLocalUpdater();
    for (int i = 0; i < 20; i++) {
      (i % 2 == 0 ? updater1 : updater2).recordTile(TileCoord.decode(i), i, List.of());
    }
    assertEquals(
      List.of(
        new TileStats.TileSummary(TileCoord.decode(19), 19, List.of()),
        new TileStats.TileSummary(TileCoord.decode(18), 18, List.of()),
        new TileStats.TileSummary(TileCoord.decode(17), 17, List.of()),
        new TileStats.TileSummary(TileCoord.decode(16), 16, List.of()),
        new TileStats.TileSummary(TileCoord.decode(15), 15, List.of()),
        new TileStats.TileSummary(TileCoord.decode(14), 14, List.of()),
        new TileStats.TileSummary(TileCoord.decode(13), 13, List.of()),
        new TileStats.TileSummary(TileCoord.decode(12), 12, List.of()),
        new TileStats.TileSummary(TileCoord.decode(11), 11, List.of()),
        new TileStats.TileSummary(TileCoord.decode(10), 10, List.of())
      ),
      tileStats.summary().get().biggestTiles()
    );
  }

  @Test
  void topLayerTiles() {
    var tileStats = new TileStats();
    var updater1 = tileStats.threadLocalUpdater();
    var updater2 = tileStats.threadLocalUpdater();
    List<TileStats.TileSummary> summaries = new ArrayList<>();
    for (int i = 0; i < 20; i++) {
      var summary = new TileStats.TileSummary(TileCoord.decode(i), i, List.of(
        new TileStats.LayerStats("a", i * 2, i, 0, 0, 0),
        new TileStats.LayerStats("b", i * 3, i, 0, 0, 0)
      ));
      summaries.add(0, summary);
      (i % 2 == 0 ? updater1 : updater2).recordTile(summary.coord(), summary.size(), summary.layers());
    }
    assertEquals(
      summaries.stream().map(d -> d.withSize(d.coord().encoded() * 2)).limit(10).toList(),
      tileStats.summary().get("a").biggestTiles()
    );
    assertEquals(
      summaries.stream().map(d -> d.withSize(d.coord().encoded() * 3)).limit(10).toList(),
      tileStats.summary().get("b").biggestTiles()
    );
  }
}
