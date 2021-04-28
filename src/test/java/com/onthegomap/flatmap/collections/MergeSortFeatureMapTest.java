package com.onthegomap.flatmap.collections;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public abstract class MergeSortFeatureMapTest {

  @TempDir
  tmp

  @Test

  public void test() {

  }

//  private MergeSortFeatureMap features;
//
//  @Before
//  public void setup() {
//    this.features = getMap(new Profile.NullProfile());
//  }
//
//  protected abstract MergeSortFeatureMap getMap(Profile profile);
//
//  @TempDir
//  Path tmpDir;
//
//  @Test
//  public void testEmpty() {
//    features.sort();
//    assertFalse(features.iterator().hasNext());
//  }
//
//  @Test
//  public void testThrowsWhenPreparedOutOfOrder() {
//    features.accept(new RenderedFeature(1, new byte[]{}));
//    assertThrows(IllegalStateException.class, features::iterator);
//    features.sort();
//    assertThrows(IllegalStateException.class, () -> features.accept(new RenderedFeature(1, new byte[]{})));
//  }
//
//  @Test
//  public void test() {
//    features.accept(FeatureMapKey.);
//    features.sort();
//    var actual = StreamSupport.stream(features.spliterator(), false).toList();
//    assertEquals(List.of(
//      new TileFeatures().add
//    ), actual);
//  }
//
//  public static class TwoWorkers extends MergeSortFeatureMapTest {
//
//    @Override
//    protected MergeSortFeatureMap getMap(Profile profile) {
//      return new MergeSortFeatureMap(tmpDir, profile, 2, 1_000, new InMemory());
//    }
//  }
//
//  public static class MergeSortOnePerFileFeatureMapTest extends MergeSortFeatureMapTest {
//
//    @Override
//    protected MergeSortFeatureMap getMap(Profile profile) {
//      return new MergeSortFeatureMap(tmpDir, profile, 2, 1, new InMemory());
//    }
//  }
//
//
//  public static class MergeSortOnePerFileOneWorkerFeatureMapTest extends MergeSortFeatureMapTest {
//
//    @Override
//    protected MergeSortFeatureMap getMap(Profile profile) {
//      return new MergeSortFeatureMap(tmpDir, profile, 1, 1_000_00, new InMemory());
//    }
//  }
//
//  public static class FeatureMapKeyTest {
//
//    @TestFactory
//    public List<DynamicTest> testEncodeLongKey() {
//      List<TileCoord> tiles = List.of(
//        TileCoord.ofXYZ(0, 0, 14),
//        TileCoord.ofXYZ((1 << 14) - 1, (1 << 14) - 1, 14),
//        TileCoord.ofXYZ(0, 0, 0),
//        TileCoord.ofXYZ(0, 0, 7),
//        TileCoord.ofXYZ((1 << 7) - 1, (1 << 7) - 1, 7)
//      );
//      List<Byte> layers = List.of((byte) 0, (byte) 1, (byte) 255);
//      List<Integer> zOrders = List.of((1 << 22) - 1, 0, -(1 << 22));
//      List<Boolean> hasGroups = List.of(false, true);
//      List<DynamicTest> result = new ArrayList<>();
//      for (TileCoord tile : tiles) {
//        for (byte layer : layers) {
//          for (int zOrder : zOrders) {
//            for (boolean hasGroup : hasGroups) {
//              FeatureMapKey key = FeatureMapKey.of(tile.encoded(), layer, zOrder, hasGroup);
//              result.add(dynamicTest(key.toString(), () -> {
//                FeatureMapKey decoded = FeatureMapKey.decode(key.encoded());
//                assertEquals(decoded.tile(), tile.encoded(), "tile");
//                assertEquals(decoded.layer(), layer, "layer");
//                assertEquals(decoded.zOrder(), zOrder, "zOrder");
//                assertEquals(decoded.hasGroup(), hasGroup, "hasGroup");
//              }));
//            }
//          }
//        }
//      }
//      return result;
//    }
//
//    @ParameterizedTest
//    @CsvSource({
//      "0,0,-2,true,   0,0,-1,false",
//      "0,0,1,false,   0,0,2,false",
//      "0,0,-1,false,  0,0,1,false",
//      "-1,0,-2,false, -1,0,-1,false",
//      "-1,0,1,false,  -1,0,2,false",
//      "-1,0,-1,false, -1,0,1,false",
//      "-1,0,-1,false, -1,0,-1,true",
//      "1,0,1,false,   1,0,1,true"
//    })
//    public void testEncodeLongKeyOrdering(
//      int tileA, byte layerA, int zOrderA, boolean hasGroupA,
//      int tileB, byte layerB, int zOrderB, boolean hasGroupB
//    ) {
//      assertTrue(
//        FeatureMapKey.encode(tileA, layerA, zOrderA, hasGroupA)
//          <
//          FeatureMapKey.encode(tileB, layerB, zOrderB, hasGroupB)
//      );
//    }
//  }
}
