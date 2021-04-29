package com.onthegomap.flatmap.collections;

import static com.onthegomap.flatmap.TestUtils.newPoint;
import static com.onthegomap.flatmap.TestUtils.newPointWithUserData;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import com.onthegomap.flatmap.Profile;
import com.onthegomap.flatmap.VectorTileEncoder;
import com.onthegomap.flatmap.geo.TileCoord;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.locationtech.jts.geom.Geometry;

public class MergeSortFeatureMapTest {

  private final List<MergeSort.Entry> list = new ArrayList<>();
  private final MergeSort sorter = new MergeSort() {
    @Override
    public void sort() {
      list.sort(Comparator.naturalOrder());
    }

    @Override
    public void add(Entry newEntry) {
      list.add(newEntry);
    }

    @Override
    public long getStorageSize() {
      return 0;
    }

    @NotNull
    @Override
    public Iterator<Entry> iterator() {
      return list.iterator();
    }
  };
  private MergeSortFeatureMap features = new MergeSortFeatureMap(sorter, new Profile.NullProfile());

  @Test
  public void testEmpty() {
    sorter.sort();
    assertFalse(features.iterator().hasNext());
  }

  long id = 0;

  private void put(int tile, String layer, Map<String, Object> attrs, Geometry geom) {
    MergeSort.Entry key = features.encode(id++, TileCoord.decode(tile), layer, attrs, geom, 0, false, 0);
    features.accept(key);
  }

  private void putWithZorder(int tile, String layer, Map<String, Object> attrs, Geometry geom, int zOrder) {
    MergeSort.Entry key = features.encode(id++, TileCoord.decode(tile), layer, attrs, geom, zOrder, false, 0);
    features.accept(key);
  }

  private void putWithGroup(int tile, String layer, Map<String, Object> attrs, Geometry geom, int zOrder, int limit) {
    MergeSort.Entry key = features.encode(id++, TileCoord.decode(tile), layer, attrs, geom, zOrder, true, limit);
    features.accept(key);
  }

  private Map<Integer, Map<String, List<Feature>>> getFeatures() {
    Map<Integer, Map<String, List<Feature>>> map = new TreeMap<>();
    for (MergeSortFeatureMap.TileFeatures tile : features) {
      for (var feature : VectorTileEncoder.decode(tile.getTile().encode())) {
        map.computeIfAbsent(tile.coord().encoded(), (i) -> new TreeMap<>())
          .computeIfAbsent(feature.layerName(), l -> new ArrayList<>())
          .add(new Feature(feature.attributes(), feature.geometry()));
      }
    }
    return map;
  }

  private static record Feature(Map<String, Object> attrs, Geometry geom) {

  }

  @Test
  public void testPutPoints() {
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
  public void testPutPointsWithZorder() {
    putWithZorder(
      1, "layer", Map.of("id", 1), newPoint(1, 2), 2
    );
    putWithZorder(
      1, "layer", Map.of("id", 2), newPoint(3, 4), 1
    );
    sorter.sort();
    assertEquals(new TreeMap<>(Map.of(
      1, new TreeMap<>(Map.of(
        "layer", List.of(
          // order reversed because of z-order
          new Feature(Map.of("id", 2L), newPoint(3, 4)),
          new Feature(Map.of("id", 1L), newPoint(1, 2))
        )
      )))), getFeatures());
  }

  @Test
  public void testLimitPoints() {
    int x = 5, y = 6;
    putWithGroup(
      1, "layer", Map.of("id", 3), newPointWithUserData(x, y, 1), 0, 2
    );
    putWithGroup(
      1, "layer", Map.of("id", 1), newPointWithUserData(1, 2, 1), 2, 2
    );
    putWithGroup(
      1, "layer", Map.of("id", 2), newPointWithUserData(3, 4, 1), 1, 2
    );
    sorter.sort();
    assertEquals(new TreeMap<>(Map.of(
      1, new TreeMap<>(Map.of(
        "layer", List.of(
          // order reversed because of z-order
          // id=3 omitted because past limit
          new Feature(Map.of("id", 2L), newPoint(3, 4)),
          new Feature(Map.of("id", 1L), newPoint(1, 2))
        )
      )))), getFeatures());
  }

  @Test
  public void testLimitPointsInDifferentGroups() {
    int x = 5, y = 6;
    putWithGroup(
      1, "layer", Map.of("id", 3), newPointWithUserData(x, y, 2), 0, 2
    );
    putWithGroup(
      1, "layer", Map.of("id", 1), newPointWithUserData(1, 2, 1), 2, 2
    );
    putWithGroup(
      1, "layer", Map.of("id", 2), newPointWithUserData(3, 4, 1), 1, 2
    );
    sorter.sort();
    assertEquals(new TreeMap<>(Map.of(
      1, new TreeMap<>(Map.of(
        "layer", List.of(
          // order reversed because of z-order
          new Feature(Map.of("id", 3L), newPoint(x, y)),
          new Feature(Map.of("id", 2L), newPoint(3, 4)),
          new Feature(Map.of("id", 1L), newPoint(1, 2))
        )
      )))), getFeatures());
  }

  @Test
  public void testDontLimitPointsWithGroup() {
    int x = 5, y = 6;
    putWithGroup(
      1, "layer", Map.of("id", 3), newPointWithUserData(x, y, 1), 0, 0
    );
    putWithGroup(
      1, "layer", Map.of("id", 1), newPointWithUserData(1, 2, 1), 2, 0
    );
    putWithGroup(
      1, "layer", Map.of("id", 2), newPointWithUserData(3, 4, 1), 1, 0
    );
    sorter.sort();
    assertEquals(new TreeMap<>(Map.of(
      1, new TreeMap<>(Map.of(
        "layer", List.of(
          // order reversed because of z-order,
          new Feature(Map.of("id", 3L), newPoint(x, y)),
          new Feature(Map.of("id", 2L), newPoint(3, 4)),
          new Feature(Map.of("id", 1L), newPoint(1, 2))
        )
      )))), getFeatures());
  }

  @Test
  public void testProfileChangesGeometry() {
    features = new MergeSortFeatureMap(sorter, new Profile.NullProfile() {
      @Override
      public List<VectorTileEncoder.VectorTileFeature> postProcessLayerFeatures(String layer, int zoom,
        List<VectorTileEncoder.VectorTileFeature> items) {
        Collections.reverse(items);
        return items;
      }
    });
    int x = 5, y = 6;
    putWithGroup(
      1, "layer", Map.of("id", 3), newPointWithUserData(x, y, 1), 0, 2
    );
    putWithGroup(
      1, "layer", Map.of("id", 1), newPointWithUserData(1, 2, 1), 2, 2
    );
    putWithGroup(
      1, "layer", Map.of("id", 2), newPointWithUserData(3, 4, 1), 1, 2
    );
    sorter.sort();
    assertEquals(new TreeMap<>(Map.of(
      1, new TreeMap<>(Map.of(
        "layer", List.of(
          // back to same order because profile reversed
          new Feature(Map.of("id", 1L), newPoint(1, 2)),
          new Feature(Map.of("id", 2L), newPoint(3, 4))
        )
      )))), getFeatures());
  }

  @TestFactory
  public List<DynamicTest> testEncodeLongKey() {
    List<TileCoord> tiles = List.of(
      TileCoord.ofXYZ(0, 0, 14),
      TileCoord.ofXYZ((1 << 14) - 1, (1 << 14) - 1, 14),
      TileCoord.ofXYZ(0, 0, 0),
      TileCoord.ofXYZ(0, 0, 7),
      TileCoord.ofXYZ((1 << 7) - 1, (1 << 7) - 1, 7)
    );
    List<Byte> layers = List.of((byte) 0, (byte) 1, (byte) 255);
    List<Integer> zOrders = List.of((1 << 22) - 1, 0, -(1 << 22));
    List<Boolean> hasGroups = List.of(false, true);
    List<DynamicTest> result = new ArrayList<>();
    for (TileCoord tile : tiles) {
      for (byte layer : layers) {
        for (int zOrder : zOrders) {
          for (boolean hasGroup : hasGroups) {
            MergeSortFeatureMap.FeatureMapKey key = MergeSortFeatureMap.FeatureMapKey
              .of(tile.encoded(), layer, zOrder, hasGroup);
            result.add(dynamicTest(key.toString(), () -> {
              MergeSortFeatureMap.FeatureMapKey decoded = MergeSortFeatureMap.FeatureMapKey.decode(key.encoded());
              assertEquals(decoded.tile(), tile, "tile");
              assertEquals(decoded.layer(), layer, "layer");
              assertEquals(decoded.zOrder(), zOrder, "zOrder");
              assertEquals(decoded.hasGroup(), hasGroup, "hasGroup");
            }));
          }
        }
      }
    }
    return result;
  }

  @ParameterizedTest
  @CsvSource({
    "0,0,-1,true,   0,0,-2,false",
    "0,0,2,false,   0,0,1,false",
    "0,0,1,false,  0,0,-1,false",
    "-1,0,-1,false, -1,0,-2,false",
    "-1,0,2,false,  -1,0,1,false",
    "-1,0,1,false, -1,0,-1,false",
    "-1,0,-1,false, -1,0,-1,true",
    "1,0,1,false,   1,0,1,true"
  })
  public void testEncodeLongKeyOrdering(
    int tileA, byte layerA, int zOrderA, boolean hasGroupA,
    int tileB, byte layerB, int zOrderB, boolean hasGroupB
  ) {
    assertTrue(
      MergeSortFeatureMap.FeatureMapKey.encode(tileA, layerA, zOrderA, hasGroupA)
        <
        MergeSortFeatureMap.FeatureMapKey.encode(tileB, layerB, zOrderB, hasGroupB)
    );
  }
}
