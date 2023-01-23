package com.onthegomap.planetiler.collection;

import static com.onthegomap.planetiler.TestUtils.decodeSilently;
import static com.onthegomap.planetiler.TestUtils.newPoint;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.VectorTile;
import com.onthegomap.planetiler.geo.GeometryType;
import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.geo.TileOrder;
import com.onthegomap.planetiler.render.RenderedFeature;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.util.CloseableConsumer;
import com.onthegomap.planetiler.util.Gzip;
import com.onthegomap.planetiler.writer.TileArchiveWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.provider.CsvSource;
import org.locationtech.jts.geom.Geometry;

class FeatureGroupTest {

  private final FeatureSort sorter = FeatureSort.newInMemory();

  private FeatureGroup features =
    new FeatureGroup(sorter, TileOrder.TMS, new Profile.NullProfile(), Stats.inMemory());
  private CloseableConsumer<SortableFeature> featureWriter = features.writerForThread();

  @Test
  void testEmpty() {
    sorter.sort();
    assertFalse(features.iterator().hasNext());
  }

  long id = 0;

  private void put(int tile, String layer, Map<String, Object> attrs, Geometry geom) {
    putWithSortKey(tile, layer, attrs, geom, 0);
  }

  private void putWithSortKey(int tile, String layer, Map<String, Object> attrs, Geometry geom, int sortKey) {
    putWithGroupAndSortKey(tile, layer, attrs, geom, sortKey, false, 0, 0);
  }

  private void putWithGroup(int tile, String layer, Map<String, Object> attrs, Geometry geom, int sortKey, long group,
    int limit) {
    putWithGroupAndSortKey(tile, layer, attrs, geom, sortKey, true, group, limit);
  }

  private void putWithGroupAndSortKey(int tile, String layer, Map<String, Object> attrs, Geometry geom, int sortKey,
    boolean hasGroup, long group, int limit) {
    putWithIdGroupAndSortKey(id++, tile, layer, attrs, geom, sortKey, hasGroup, group, limit);
  }

  private void putWithIdGroupAndSortKey(long id, int tile, String layer, Map<String, Object> attrs, Geometry geom,
    int sortKey, boolean hasGroup, long group, int limit) {
    RenderedFeature feature = new RenderedFeature(
      TileCoord.decode(tile),
      new VectorTile.Feature(layer, id, VectorTile.encodeGeometry(geom), attrs),
      sortKey,
      hasGroup ? Optional.of(new RenderedFeature.Group(group, limit)) : Optional.empty()
    );
    featureWriter.accept(features.newRenderedFeatureEncoder().apply(feature));
  }

  private void put(PuTileArgs args) {
    putWithIdGroupAndSortKey(args.id(), args.tile(), args.layer(), args.attrs(), args.geom(), args.sortKey(),
      args.hasGroup(), args.group(), args.limit());
  }

  private Map<Integer, Map<String, List<Feature>>> getFeatures() {
    Map<Integer, Map<String, List<Feature>>> map = new TreeMap<>();
    for (FeatureGroup.TileFeatures tile : features) {
      for (var feature : VectorTile.decode(tile.getVectorTileEncoder().encode())) {
        map.computeIfAbsent(tile.tileCoord().encoded(), (i) -> new TreeMap<>())
          .computeIfAbsent(feature.layer(), l -> new ArrayList<>())
          .add(new Feature(feature.attrs(), decodeSilently(feature.geometry())));
      }
    }
    return map;
  }


  private Map<Integer, Map<String, List<Feature>>> getFeaturesParallel() {
    Map<Integer, Map<String, List<Feature>>> map = new TreeMap<>();
    var reader = features.parallelIterator(2);
    for (FeatureGroup.TileFeatures tile : reader.result()) {
      for (var feature : VectorTile.decode(tile.getVectorTileEncoder().encode())) {
        map.computeIfAbsent(tile.tileCoord().encoded(), (i) -> new TreeMap<>())
          .computeIfAbsent(feature.layer(), l -> new ArrayList<>())
          .add(new Feature(feature.attrs(), decodeSilently(feature.geometry())));
      }
    }
    return map;
  }

  private record Feature(Map<String, Object> attrs, Geometry geom) {}

  @Test
  void testPutPoints() {
    put(3, "layer3", Map.of("a", 1.5d, "b", "string"), newPoint(5, 6));
    put(3, "layer4", Map.of("a", 1.5d, "b", "string"), newPoint(5, 6));
    put(2, "layer", Map.of("a", 1.5d, "b", "string"), newPoint(5, 6));
    put(1, "layer", Map.of("a", 1, "b", 2L), newPoint(1, 2));
    put(1, "layer2", Map.of("c", 3d, "d", true), newPoint(3, 4));
    sorter.sort();
    assertEquals(new TreeMap<>(Map.of(
      1, new TreeMap<>(Map.of(
        "layer", List.of(
          new Feature(Map.of("a", 1L, "b", 2L), newPoint(1, 2))
        ),
        "layer2", List.of(
          new Feature(Map.of("c", 3d, "d", true), newPoint(3, 4))
        )
      )), 2, new TreeMap<>(Map.of(
        "layer", List.of(
          new Feature(Map.of("a", 1.5d, "b", "string"), newPoint(5, 6))
        )
      )), 3, new TreeMap<>(Map.of(
        "layer3", List.of(
          new Feature(Map.of("a", 1.5d, "b", "string"), newPoint(5, 6))
        ),
        "layer4", List.of(
          new Feature(Map.of("a", 1.5d, "b", "string"), newPoint(5, 6))
        )
      )))), getFeatures());
  }

  @Test
  void testShardedRead() {
    put(3, "layer3", Map.of("a", 1.5d, "b", "string"), newPoint(5, 6));
    put(3, "layer4", Map.of("a", 1.5d, "b", "string"), newPoint(5, 6));
    put(2, "layer", Map.of("a", 1.5d, "b", "string"), newPoint(5, 6));
    put(1, "layer", Map.of("a", 1, "b", 2L), newPoint(1, 2));
    put(1, "layer2", Map.of("c", 3d, "d", true), newPoint(3, 4));
    sorter.sort();
    assertEquals(new TreeMap<>(Map.of(
      1, new TreeMap<>(Map.of(
        "layer", List.of(
          new Feature(Map.of("a", 1L, "b", 2L), newPoint(1, 2))
        ),
        "layer2", List.of(
          new Feature(Map.of("c", 3d, "d", true), newPoint(3, 4))
        )
      )), 2, new TreeMap<>(Map.of(
        "layer", List.of(
          new Feature(Map.of("a", 1.5d, "b", "string"), newPoint(5, 6))
        )
      )), 3, new TreeMap<>(Map.of(
        "layer3", List.of(
          new Feature(Map.of("a", 1.5d, "b", "string"), newPoint(5, 6))
        ),
        "layer4", List.of(
          new Feature(Map.of("a", 1.5d, "b", "string"), newPoint(5, 6))
        )
      )))), getFeaturesParallel());
  }

  @Test
  void testPutPointsWithSortKey() {
    putWithSortKey(
      1, "layer", Map.of("id", 1), newPoint(1, 2), 2
    );
    putWithSortKey(
      1, "layer", Map.of("id", 2), newPoint(3, 4), 1
    );
    sorter.sort();
    assertEquals(new TreeMap<>(Map.of(
      1, new TreeMap<>(Map.of(
        "layer", List.of(
          // order reversed because of sort-key
          new Feature(Map.of("id", 2L), newPoint(3, 4)),
          new Feature(Map.of("id", 1L), newPoint(1, 2))
        )
      )))), getFeatures());
  }

  @Test
  void testLimitPoints() {
    int x = 5, y = 6;
    putWithGroup(
      1, "layer", Map.of("id", 3), newPoint(x, y), 2, 1, 2
    );
    putWithGroup(
      1, "layer", Map.of("id", 2), newPoint(3, 4), 1, 1, 2
    );
    putWithGroup(
      1, "layer", Map.of("id", 1), newPoint(1, 2), 0, 1, 2
    );
    sorter.sort();
    assertEquals(new TreeMap<>(Map.of(
      1, new TreeMap<>(Map.of(
        "layer", List.of(
          // id=3 omitted because past limit
          // sorted by sortKey ascending
          new Feature(Map.of("id", 1L), newPoint(1, 2)),
          new Feature(Map.of("id", 2L), newPoint(3, 4))
        )
      )))), getFeatures());
  }

  @Test
  void testLimitPointsInDifferentGroups() {
    int x = 5, y = 6;
    putWithGroup(
      1, "layer", Map.of("id", 3), newPoint(x, y), 0, 2, 2
    );
    putWithGroup(
      1, "layer", Map.of("id", 1), newPoint(1, 2), 2, 1, 2
    );
    putWithGroup(
      1, "layer", Map.of("id", 2), newPoint(3, 4), 1, 1, 2
    );
    sorter.sort();
    assertEquals(new TreeMap<>(Map.of(
      1, new TreeMap<>(Map.of(
        "layer", List.of(
          // ordered by sort key
          new Feature(Map.of("id", 3L), newPoint(x, y)),
          new Feature(Map.of("id", 2L), newPoint(3, 4)),
          new Feature(Map.of("id", 1L), newPoint(1, 2))
        )
      )))), getFeatures());
  }

  @Test
  void testDontLimitPointsWithGroup() {
    int x = 5, y = 6;
    putWithGroup(
      1, "layer", Map.of("id", 3), newPoint(x, y), 0, 1, 0
    );
    putWithGroup(
      1, "layer", Map.of("id", 1), newPoint(1, 2), 2, 1, 0
    );
    putWithGroup(
      1, "layer", Map.of("id", 2), newPoint(3, 4), 1, 1, 0
    );
    sorter.sort();
    assertEquals(new TreeMap<>(Map.of(
      1, new TreeMap<>(Map.of(
        "layer", List.of(
          // order reversed because of sort-key,
          new Feature(Map.of("id", 3L), newPoint(x, y)),
          new Feature(Map.of("id", 2L), newPoint(3, 4)),
          new Feature(Map.of("id", 1L), newPoint(1, 2))
        )
      )))), getFeatures());
  }

  @Test
  void testProfileChangesGeometry() {
    features = new FeatureGroup(sorter, TileOrder.TMS, new Profile.NullProfile() {
      @Override
      public List<VectorTile.Feature> postProcessLayerFeatures(String layer, int zoom, List<VectorTile.Feature> items) {
        Collections.reverse(items);
        return items;
      }
    }, Stats.inMemory());
    featureWriter = features.writerForThread();
    putWithGroup(
      1, "layer", Map.of("id", 3), newPoint(5, 6), 2, 1, 2
    );
    putWithGroup(
      1, "layer", Map.of("id", 1), newPoint(1, 2), 0, 1, 2
    );
    putWithGroup(
      1, "layer", Map.of("id", 2), newPoint(3, 4), 1, 1, 2
    );
    sorter.sort();
    assertEquals(Map.of(
      1, Map.of(
        "layer", List.of(
          // not sorted by sortKey asc because profile reversed it
          new Feature(Map.of("id", 2L), newPoint(3, 4)),
          new Feature(Map.of("id", 1L), newPoint(1, 2))
        )
      )), getFeatures());
  }

  @Test
  void testHilbertOrdering() {
    features = new FeatureGroup(sorter, TileOrder.HILBERT, new Profile.NullProfile() {}, Stats.inMemory());
    featureWriter = features.writerForThread();

    // TMS tile IDs at zoom level 1:
    // 2 4
    // 1 3

    put(
      1, "layer", Map.of("id", 1), newPoint(0, 0)
    );
    put(
      2, "layer", Map.of("id", 2), newPoint(0, 0)
    );
    put(
      3, "layer", Map.of("id", 3), newPoint(0, 0)
    );
    put(
      4, "layer", Map.of("id", 4), newPoint(0, 0)
    );

    // calls sort()
    var iter = features.iterator();

    var tile = iter.next().tileCoord();
    assertEquals(0, tile.x());
    assertEquals(0, tile.y());
    tile = iter.next().tileCoord();
    assertEquals(0, tile.x());
    assertEquals(1, tile.y());
    tile = iter.next().tileCoord();
    assertEquals(1, tile.x());
    assertEquals(1, tile.y());
    tile = iter.next().tileCoord();
    assertEquals(1, tile.x());
    assertEquals(0, tile.y());
  }

  @TestFactory
  List<DynamicTest> testEncodeLongKey() {
    List<TileCoord> tiles = List.of(
      TileCoord.ofXYZ(0, 0, 14),
      TileCoord.ofXYZ((1 << 14) - 1, (1 << 14) - 1, 14),
      TileCoord.ofXYZ(0, 0, 0),
      TileCoord.ofXYZ(0, 0, 7),
      TileCoord.ofXYZ((1 << 7) - 1, (1 << 7) - 1, 7)
    );
    List<Byte> layers = List.of((byte) 0, (byte) 1, (byte) 255);
    List<Integer> sortKeys = List.of(-(1 << 22), 0, (1 << 22) - 1);
    List<Boolean> hasGroups = List.of(false, true);
    List<DynamicTest> result = new ArrayList<>();
    for (TileCoord tile : tiles) {
      for (byte layer : layers) {
        for (int sortKey : sortKeys) {
          for (boolean hasGroup : hasGroups) {
            long key = FeatureGroup.encodeKey(tile.encoded(), layer, sortKey, hasGroup);
            result.add(dynamicTest(tile + " " + layer + " " + sortKey + " " + hasGroup, () -> {
              assertEquals(tile.encoded(), FeatureGroup.extractTileFromKey(key), "tile");
              assertEquals(layer, FeatureGroup.extractLayerIdFromKey(key), "layer");
              assertEquals(sortKey, FeatureGroup.extractSortKeyFromKey(key), "sortKey");
              assertEquals(hasGroup, FeatureGroup.extractHasGroupFromKey(key), "hasGroup");
            }));
          }
        }
      }
    }
    return result;
  }

  @ParameterizedTest
  @CsvSource({
    "0,0,-2,true,   0,0,-1,false",
    "0,0,1,false,   0,0,2,false",
    "0,0,-1,false,  0,0,1,false",
    "-1,0,-2,false, -1,0,-1,false",
    "-1,0,1,false,  -1,0,2,false",
    "-1,0,-1,false, -1,0,1,false",
    "-1,0,-1,false, -1,0,-1,true",
    "1,0,1,false,   1,0,1,true"
  })
  void testEncodeLongKeyOrdering(
    int tileA, byte layerA, int sortKeyA, boolean hasGroupA,
    int tileB, byte layerB, int sortKeyB, boolean hasGroupB
  ) {
    assertTrue(
      FeatureGroup.encodeKey(tileA, layerA, sortKeyA, hasGroupA) < FeatureGroup.encodeKey(tileB, layerB, sortKeyB,
        hasGroupB)
    );
  }

  @ParameterizedTest(name = "{0}")
  @ArgumentsSource(SameFeatureGroupTestArgs.class)
  void testHasSameContents(String testName, boolean expectSame, PuTileArgs args0, PuTileArgs args1) {
    put(args0);
    put(args1);
    sorter.sort();
    var iter = features.iterator();
    var tile0 = iter.next();
    var tile1 = iter.next();
    assertEquals(expectSame, tile0.hasSameContents(tile1));
  }

  @ParameterizedTest(name = "{0}")
  @ArgumentsSource(SameFeatureGroupTestArgs.class)
  void testGenerateContentHash(String testName, boolean expectSame, PuTileArgs args0, PuTileArgs args1)
    throws IOException {
    put(args0);
    put(args1);
    sorter.sort();
    var iter = features.iterator();
    var tileHash0 = TileArchiveWriter.generateContentHash(
      Gzip.gzip(iter.next().getVectorTileEncoder().encode())
    );
    var tileHash1 = TileArchiveWriter.generateContentHash(
      Gzip.gzip(iter.next().getVectorTileEncoder().encode())
    );
    if (expectSame) {
      assertEquals(tileHash0, tileHash1);
    } else {
      assertNotEquals(tileHash0, tileHash1);
    }
  }

  @ParameterizedTest
  @CsvSource({
    "UNKNOWN,0",
    "LINE,2",
    "POLYGON,15",
    "POINT,14"
  })
  void testEncodeDecodeGeometryMetadata(String geomTypeString, int scale) {
    GeometryType geomType = GeometryType.valueOf(geomTypeString);
    byte encoded = FeatureGroup.encodeGeomTypeAndScale(new VectorTile.VectorGeometry(new int[0], geomType, scale));
    assertEquals(geomType, FeatureGroup.decodeGeomType(encoded));
    assertEquals(scale, FeatureGroup.decodeScale(encoded));
  }

  private static class SameFeatureGroupTestArgs implements ArgumentsProvider {

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) throws Exception {
      return Stream.of(
        argsOf(
          "same despite diff sort key", true,
          new PuTileArgs(1, 1, "layer", Map.of("id", 1), newPoint(1, 2), 1, true, 2, 3),
          new PuTileArgs(1, 2, "layer", Map.of("id", 1), newPoint(1, 2), 2, true, 2, 3)
        ),
        argsOf(
          "diff when geometry changes", false,
          new PuTileArgs(1, 1, "layer", Map.of("id", 1), newPoint(1, 2), 1, true, 2, 3),
          new PuTileArgs(1, 2, "layer", Map.of("id", 1), newPoint(1, 3), 1, true, 2, 3)
        ),
        argsOf(
          "diff when attrs changes", false,
          new PuTileArgs(1, 1, "layer", Map.of("id", 1), newPoint(1, 2), 1, true, 2, 3),
          new PuTileArgs(1, 2, "layer", Map.of("id", 2), newPoint(1, 2), 1, true, 2, 3)
        ),
        argsOf(
          "diff when layer changes", false,
          new PuTileArgs(1, 1, "layer", Map.of("id", 1), newPoint(1, 2), 1, true, 2, 3),
          new PuTileArgs(1, 2, "layer2", Map.of("id", 1), newPoint(1, 2), 1, true, 2, 3)
        ),
        argsOf(
          "diff when id changes", false,
          new PuTileArgs(1, 1, "layer", Map.of("id", 1), newPoint(1, 2), 1, true, 2, 3),
          new PuTileArgs(2, 2, "layer", Map.of("id", 1), newPoint(1, 2), 1, true, 2, 3)
        )
      );
    }

    private static Arguments argsOf(String testName, boolean expectSame, PuTileArgs args0,
      PuTileArgs args1) {
      return Arguments.of(testName, expectSame, args0, args1);
    }
  }

  private static record PuTileArgs(long id, int tile, String layer, Map<String, Object> attrs, Geometry geom,
    int sortKey, boolean hasGroup, long group, int limit) {}
}
