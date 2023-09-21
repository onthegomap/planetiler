package com.onthegomap.planetiler.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.onthegomap.planetiler.geo.TileCoord;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class TilesetSummaryStatisticsTest {
  @Test
  void aggregateTileStats() {
    var tileStats = new TilesetSummaryStatistics();
    var updater1 = tileStats.threadLocalUpdater();
    var updater2 = tileStats.threadLocalUpdater();
    updater1.recordTile(TileCoord.ofXYZ(0, 0, 1), 123, List.of(
      new TileSizeStats.LayerStats("a", 1, 2, 3, 4, 5),
      new TileSizeStats.LayerStats("b", 6, 7, 8, 9, 10)
    ));
    updater2.recordTile(TileCoord.ofXYZ(0, 1, 1), 345, List.of(
      new TileSizeStats.LayerStats("b", 1, 2, 3, 4, 5),
      new TileSizeStats.LayerStats("c", 6, 7, 8, 9, 10)
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

    updater1.recordTile(TileCoord.ofXYZ(0, 0, 2), 0, List.of(
      new TileSizeStats.LayerStats("c", 10, 7, 8, 9, 10)
    ));
    assertEquals("""
                  z1    z2   all
              a    1     0     1
              b    6     0     6
              c    6    10    10
      """.stripTrailing(), tileStats.summary().formatTable(Number::toString, cell -> cell.maxSize()));

    assertEquals("""
                  z1    z2   all
              a    1     0     1
              b    2     0     2
              c    1     1     2
      """.stripTrailing(), tileStats.summary().formatTable(Number::toString, cell -> cell.numTiles()));
  }

  @Test
  void topGzippedTiles() {
    var tileStats = new TilesetSummaryStatistics();
    var updater1 = tileStats.threadLocalUpdater();
    var updater2 = tileStats.threadLocalUpdater();
    for (int i = 0; i < 20; i++) {
      (i % 2 == 0 ? updater1 : updater2).recordTile(TileCoord.decode(i), i, List.of());
    }
    assertEquals(
      List.of(
        new TilesetSummaryStatistics.TileSummary(TileCoord.decode(19), 19, List.of()),
        new TilesetSummaryStatistics.TileSummary(TileCoord.decode(18), 18, List.of()),
        new TilesetSummaryStatistics.TileSummary(TileCoord.decode(17), 17, List.of()),
        new TilesetSummaryStatistics.TileSummary(TileCoord.decode(16), 16, List.of()),
        new TilesetSummaryStatistics.TileSummary(TileCoord.decode(15), 15, List.of()),
        new TilesetSummaryStatistics.TileSummary(TileCoord.decode(14), 14, List.of()),
        new TilesetSummaryStatistics.TileSummary(TileCoord.decode(13), 13, List.of()),
        new TilesetSummaryStatistics.TileSummary(TileCoord.decode(12), 12, List.of()),
        new TilesetSummaryStatistics.TileSummary(TileCoord.decode(11), 11, List.of()),
        new TilesetSummaryStatistics.TileSummary(TileCoord.decode(10), 10, List.of())
      ),
      tileStats.summary().get().biggestTiles()
    );
  }

  @Test
  void topLayerTiles() {
    var tileStats = new TilesetSummaryStatistics();
    var updater1 = tileStats.threadLocalUpdater();
    var updater2 = tileStats.threadLocalUpdater();
    List<TilesetSummaryStatistics.TileSummary> summaries = new ArrayList<>();
    for (int i = 0; i < 20; i++) {
      var summary = new TilesetSummaryStatistics.TileSummary(TileCoord.decode(i), i, List.of(
        new TileSizeStats.LayerStats("a", i * 2, i, 0, 0, 0),
        new TileSizeStats.LayerStats("b", i * 3, i, 0, 0, 0)
      ));
      summaries.add(0, summary);
      (i % 2 == 0 ? updater1 : updater2).recordTile(summary.coord(), summary.archivedSize(), summary.layers());
    }
    assertEquals(
      summaries.stream().map(d -> d.withSize(d.coord().encoded() * 2)).limit(10).toList(),
      tileStats.summary().get("a").biggestTiles()
    );
    assertEquals(
      summaries.stream().map(d -> d.withSize(d.coord().encoded() * 3)).limit(10).toList(),
      tileStats.summary().get("b").biggestTiles()
    );
    assertEquals("""
      1. 2/3/1 (19) 2.5/33.25663/135 (b:57)
      2. 2/3/2 (18) 2.5/-33.25663/135 (b:54)
      3. 2/3/3 (17) 2.5/-75.78219/135 (b:51)
      4. 2/2/0 (16) 2.5/75.78219/45 (b:48)
      5. 2/2/1 (15) 2.5/33.25663/45 (b:45)
      6. 2/2/2 (14) 2.5/-33.25663/45 (b:42)
      7. 2/2/3 (13) 2.5/-75.78219/45 (b:39)
      8. 2/1/0 (12) 2.5/75.78219/-45 (b:36)
      9. 2/1/1 (11) 2.5/33.25663/-45 (b:33)
      10. 2/1/2 (10) 2.5/-33.25663/-45 (b:30)
      """.trim(), tileStats.summary().get().formatBiggestTiles("{z}/{lat}/{lon}"));
  }

  @Test
  void tileWeights() {
    var tileStats = new TilesetSummaryStatistics(new TileWeights()
      .put(TileCoord.ofXYZ(0, 0, 0), 2)
      .put(TileCoord.ofXYZ(0, 0, 1), 1));
    var updater1 = tileStats.threadLocalUpdater();
    var updater2 = tileStats.threadLocalUpdater();

    updater1.recordTile(
      TileCoord.ofXYZ(0, 0, 0),
      100,
      List.of(new TileSizeStats.LayerStats("a", 10, 0, 0, 0, 0))
    );
    updater2.recordTile(
      TileCoord.ofXYZ(0, 0, 1),
      200,
      List.of(
        new TileSizeStats.LayerStats("a", 20, 0, 0, 0, 0),
        new TileSizeStats.LayerStats("b", 30, 0, 0, 0, 0)
      )
    );
    updater2.recordTile(
      TileCoord.ofXYZ(0, 0, 2), // no stats
      400,
      List.of(
        new TileSizeStats.LayerStats("c", 40, 0, 0, 0, 0)
      )
    );

    assertEquals(
      (100 * 2 + 200) / 3d,
      tileStats.summary().get().weightedAverageArchivedSize()
    );
    assertEquals(
      (10 * 2 + 20) / 3d,
      tileStats.summary().get("a").weightedAverageSize()
    );
    assertEquals(
      30d,
      tileStats.summary().get("b").weightedAverageSize()
    );
    assertEquals(
      40d,
      tileStats.summary().get("c").weightedAverageSize()
    );
  }

  @Test
  void tileWeightsScaledByZoom() {
    var tileStats = new TilesetSummaryStatistics(new TileWeights()
      .put(TileCoord.ofXYZ(0, 0, 0), 90)
      .put(TileCoord.ofXYZ(0, 0, 1), 8)
      .put(TileCoord.ofXYZ(1, 0, 1), 2)
      .put(TileCoord.ofXYZ(1, 0, 2), 50));
    var updater1 = tileStats.threadLocalUpdater();
    var updater2 = tileStats.threadLocalUpdater();

    updater1.recordTile(
      TileCoord.ofXYZ(0, 0, 0),
      100,
      List.of(new TileSizeStats.LayerStats("a", 10, 0, 0, 0, 0))
    );
    updater2.recordTile(
      TileCoord.ofXYZ(0, 0, 1),
      200,
      List.of(
        new TileSizeStats.LayerStats("a", 20, 0, 0, 0, 0),
        new TileSizeStats.LayerStats("b", 30, 0, 0, 0, 0)
      )
    );

    // z0 90%     100/10 (a:10)
    // z1 10% (all)
    //     8% 0,0 200/50 (a:20, b:30)
    // z2 - ignore z2 since we don't have an tiles there

    // even though we're missing some tiles at z1, z1 should still get 10%
    assertEquals(
      0.9 * 100 + 0.1 * 200,
      tileStats.summary().get().weightedAverageArchivedSize(),
      1.5d
    );
    assertEquals(
      0.9 * 10 + 0.1 * 50,
      tileStats.summary().get().weightedAverageSize(),
      0.2
    );

    assertEquals(
      0.9 * 10 + 0.1 * 20,
      tileStats.summary().get("a").weightedAverageSize(),
      0.2
    );
    assertEquals(
      30,
      tileStats.summary().get("b").weightedAverageSize(),
      1e-5
    );

    assertEquals(
      200,
      tileStats.summary().get(1).weightedAverageArchivedSize(),
      1e-5
    );
    assertEquals(
      50,
      tileStats.summary().get(1).weightedAverageSize(),
      1e-5
    );
  }
}
