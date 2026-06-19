package com.onthegomap.planetiler.util;

import static com.onthegomap.planetiler.TestUtils.newPoint;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.onthegomap.planetiler.VectorTile;
import com.onthegomap.planetiler.geo.TileCoord;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.maplibre.mlt.converter.ConversionConfig;
import org.maplibre.mlt.converter.FeatureTableOptimizations;
import org.maplibre.mlt.converter.MltConverter;
import org.maplibre.mlt.converter.mvt.ColumnMapping;
import org.maplibre.mlt.converter.mvt.ColumnMappingConfig;
import org.maplibre.mlt.converter.mvt.MapboxVectorTile;
import org.maplibre.mlt.decoder.MltDecoder;

class TileSizeStatsTest {
  @Test
  void computeStatsEmpty() {
    var stats = TileSizeStats.computeTileStats(new VectorTile().toProto());
    assertEquals(0, stats.size());
  }

  @Test
  void computeStatsOneFeature() throws IOException {
    var stats = TileSizeStats.computeTileStats(new VectorTile()
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

    var formatted = TileSizeStats.newThreadLocalSerializer().formatOutputRows(TileCoord.ofXYZ(1, 2, 3), 999, stats);
    assertEquals(
      """
        z	x	y	hilbert	archived_tile_bytes	layer	layer_bytes	layer_features	layer_geometries	layer_attr_bytes	layer_attr_keys	layer_attr_values
        3	1	2	34	999	layer	55	1	1	18	2	2
        """
        .trim(),
      (TileSizeStats.headerRow() + String.join("", formatted)).trim());
  }

  @Test
  void computeStats2Features() throws IOException {
    var stats = TileSizeStats.computeTileStats(new VectorTile()
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

    var formatted = TileSizeStats.newThreadLocalSerializer().formatOutputRows(TileCoord.ofXYZ(1, 2, 3), 999, stats);
    assertEquals(
      """
        z	x	y	hilbert	archived_tile_bytes	layer	layer_bytes	layer_features	layer_geometries	layer_attr_bytes	layer_attr_keys	layer_attr_values
        3	1	2	34	999	a	72	2	2	20	2	3
        3	1	2	34	999	b	19	1	1	0	0	0
        """
        .trim(),
      (TileSizeStats.headerRow() + String.join("", formatted)).trim());
  }

  @Test
  void computeStats2FeaturesMlt() throws IOException {
    VectorTile vectorTile = new VectorTile()
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
          Map.of("key1", "value1", "key2", 3)
        )
      ));
    MapboxVectorTile mltInput = vectorTile.toMltInput();
    ColumnMappingConfig columnMappings = new ColumnMappingConfig();
    var tilesetMetadata = MltConverter.createTilesetMetadata(mltInput, columnMappings, true);
    Map<String, FeatureTableOptimizations> optimizations = Map.of();
    var conversionConfig = ConversionConfig.builder().includeIds(true).useFSST(false).useFastPFOR(false)
      .optimizations(optimizations).build();
    var mlt = MltConverter.convertMvt(mltInput, tilesetMetadata, conversionConfig, null);
    var stats = TileSizeStats.computeMltTileStats(vectorTile, mltInput, mlt);
    assertEquals(2, stats.size());
    var entry1 = stats.getFirst();
    assertEquals("a", entry1.layer());
    assertEquals(2, entry1.layerFeatures());
    assertEquals(85, entry1.layerBytes());

    assertEquals(41, entry1.layerAttrBytes());
    assertEquals(2, entry1.layerAttrKeys());
    assertEquals(3, entry1.layerAttrValues());
    var entry2 = stats.get(1);
    assertEquals("b", entry2.layer());
    assertEquals(1, entry2.layerFeatures());

    var formatted = TileSizeStats.newThreadLocalSerializer().formatOutputRows(TileCoord.ofXYZ(1, 2, 3), 999, stats);
    assertEquals(
      """
        z	x	y	hilbert	archived_tile_bytes	layer	layer_bytes	layer_features	layer_geometries	layer_attr_bytes	layer_attr_keys	layer_attr_values
        3	1	2	34	999	a	85	2	2	41	2	3
        3	1	2	34	999	b	25	1	1	0	0	0
        """
        .trim(),
      (TileSizeStats.headerRow() + String.join("", formatted)).trim());
  }

  @Test
  void issue1470_computeMltFastPforStats() throws IOException {
    try (var is = Objects.requireNonNull(getClass().getResourceAsStream("/fastpfor.mlt"))) {
      var bytes = is.readAllBytes();
      var mlt = MltDecoder.decodeMlTile(bytes);
      var mvt = new MapboxVectorTile(mlt.layers());
      TileSizeStats.computeMltTileStats(null, mvt, bytes);
    }
  }

  @Test
  void computeStats2FeaturesNested() throws IOException {
    VectorTile vectorTile = new VectorTile()
      .addLayerFeatures("a", List.of(
        new VectorTile.Feature(
          "a",
          1,
          VectorTile.encodeGeometry(newPoint(0, 0)),
          Map.of("key:1", "value1", "key:2", "value2")
        ),
        new VectorTile.Feature(
          "a",
          2,
          VectorTile.encodeGeometry(newPoint(1, 1)),
          Map.of("key:1", "value2", "key:2", "value1")
        )
      ));
    MapboxVectorTile mltInput = vectorTile.toMltInput();
    ColumnMappingConfig columnMappings =
      ColumnMappingConfig.of(Pattern.compile(".*"), List.of(new ColumnMapping("key", ":", true)));
    var tilesetMetadata = MltConverter.createTilesetMetadata(mltInput, columnMappings, true);
    var conversionConfig = ConversionConfig.builder().includeIds(true).useFSST(true).useFastPFOR(false)
      .optimizations(
        Map.of("a",
          new FeatureTableOptimizations(false, false,
            List.of(new ColumnMapping("key", ":", true)))))
      .build();
    var mlt = MltConverter.convertMvt(mltInput, tilesetMetadata, conversionConfig, null);
    var stats = TileSizeStats.computeMltTileStats(vectorTile, mltInput, mlt);
    assertEquals(1, stats.size());
    var entry1 = stats.getFirst();
    assertEquals("a", entry1.layer());
    assertEquals(2, entry1.layerFeatures());
    assertEquals(95, entry1.layerBytes());

    assertEquals(50, entry1.layerAttrBytes());
    assertEquals(2, entry1.layerAttrKeys());
    assertEquals(2, entry1.layerAttrValues());

    var formatted = TileSizeStats.newThreadLocalSerializer().formatOutputRows(TileCoord.ofXYZ(1, 2, 3), 999, stats);
    assertEquals(
      """
        z	x	y	hilbert	archived_tile_bytes	layer	layer_bytes	layer_features	layer_geometries	layer_attr_bytes	layer_attr_keys	layer_attr_values
        3	1	2	34	999	a	95	2	2	50	2	2
        """
        .trim(),
      (TileSizeStats.headerRow() + String.join("", formatted)).trim());
  }

  @Test
  void testParquetOutput(@TempDir Path tempDir) throws IOException {
    Path parquetFile = tempDir.resolve("layerstats.parquet");
    var tileCoord = TileCoord.ofXYZ(1, 2, 3);
    var stats = List.of(
      new TileSizeStats.LayerStats("layer1", 100, 5, 5, 20, 2, 3),
      new TileSizeStats.LayerStats("layer2", 150, 10, 10, 30, 3, 4)
    );

    try (var writer = TileSizeStats.createWriter(parquetFile)) {
      writer.write(tileCoord, 999, stats);
    }

    assertTrue(Files.exists(parquetFile));
    assertTrue(Files.size(parquetFile) > 0);

    blue.strategic.parquet.Hydrator<Map<String, Object>, Map<String, Object>> hydrator =
      new blue.strategic.parquet.Hydrator<>() {
        @Override
        public Map<String, Object> start() {
          return new HashMap<>();
        }

        @Override
        public Map<String, Object> add(Map<String, Object> target, String heading, Object value) {
          target.put(heading, value);
          return target;
        }

        @Override
        public Map<String, Object> finish(Map<String, Object> target) {
          return target;
        }
      };

    List<Map<String, Object>> rows;
    try (var stream = blue.strategic.parquet.ParquetReader.streamContent(
      parquetFile.toFile(), blue.strategic.parquet.HydratorSupplier.constantly(hydrator))) {
      rows = stream.toList();
    }

    assertEquals(2, rows.size());

    var row0 = rows.get(0);
    assertEquals(tileCoord.z(), ((Number) row0.get("z")).intValue());
    assertEquals(tileCoord.x(), ((Number) row0.get("x")).intValue());
    assertEquals(tileCoord.y(), ((Number) row0.get("y")).intValue());
    assertEquals(tileCoord.hilbertEncoded(), ((Number) row0.get("hilbert")).longValue());
    assertEquals(999, ((Number) row0.get("archived_tile_bytes")).intValue());
    assertEquals("layer1", row0.get("layer"));
    assertEquals(100, ((Number) row0.get("layer_bytes")).intValue());
    assertEquals(5, ((Number) row0.get("layer_features")).intValue());
    assertEquals(5, ((Number) row0.get("layer_geometries")).intValue());
    assertEquals(20, ((Number) row0.get("layer_attr_bytes")).intValue());
    assertEquals(2, ((Number) row0.get("layer_attr_keys")).intValue());
    assertEquals(3, ((Number) row0.get("layer_attr_values")).intValue());

    var row1 = rows.get(1);
    assertEquals("layer2", row1.get("layer"));
    assertEquals(150, ((Number) row1.get("layer_bytes")).intValue());
    assertEquals(10, ((Number) row1.get("layer_features")).intValue());
    assertEquals(30, ((Number) row1.get("layer_attr_bytes")).intValue());
  }

}
