package com.onthegomap.flatmap;

import static com.onthegomap.flatmap.TestUtils.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.IntObjectMap;
import com.graphhopper.coll.GHIntObjectHashMap;
import com.onthegomap.flatmap.geo.GeometryException;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Geometry;

public class FeatureMergeTest {

  private VectorTileEncoder.Feature feature(long id, Geometry geom, Map<String, Object> attrs) {
    return new VectorTileEncoder.Feature(
      "layer",
      id,
      VectorTileEncoder.encodeGeometry(geom),
      attrs
    );
  }

  @Test
  public void mergeMergeZeroLineStrings() throws GeometryException {
    assertEquals(
      List.of(),
      FeatureMerge.mergeLineStrings(
        List.of(),
        0,
        0,
        0
      )
    );
  }

  @Test
  public void mergeMergeOneLineStrings() throws GeometryException {
    assertEquals(
      List.of(
        feature(1, newLineString(10, 10, 20, 20), Map.of())
      ),
      FeatureMerge.mergeLineStrings(
        List.of(
          feature(1, newLineString(10, 10, 20, 20), Map.of())
        ),
        0,
        0,
        0
      )
    );
  }

  @Test
  public void dontMergeDisconnectedLineStrings() throws GeometryException {
    assertEquals(
      List.of(
        feature(1, newMultiLineString(
          newLineString(10, 10, 20, 20),
          newLineString(30, 30, 40, 40)
        ), Map.of())
      ),
      FeatureMerge.mergeLineStrings(
        List.of(
          feature(1, newLineString(10, 10, 20, 20), Map.of()),
          feature(2, newLineString(30, 30, 40, 40), Map.of())
        ),
        0,
        0,
        0
      )
    );
  }

  @Test
  public void dontMergeConnectedLineStringsDifferentAttr() throws GeometryException {
    assertEquals(
      List.of(
        feature(1, newLineString(10, 10, 20, 20), Map.of("a", 1)),
        feature(2, newLineString(20, 20, 30, 30), Map.of("b", 2))
      ),
      FeatureMerge.mergeLineStrings(
        List.of(
          feature(1, newLineString(10, 10, 20, 20), Map.of("a", 1)),
          feature(2, newLineString(20, 20, 30, 30), Map.of("b", 2))
        ),
        0,
        0,
        0
      )
    );
  }

  @Test
  public void mergeConnectedLineStringsSameAttrs() throws GeometryException {
    assertEquals(
      List.of(
        feature(1, newLineString(10, 10, 30, 30), Map.of("a", 1))
      ),
      FeatureMerge.mergeLineStrings(
        List.of(
          feature(1, newLineString(10, 10, 20, 20), Map.of("a", 1)),
          feature(2, newLineString(20, 20, 30, 30), Map.of("a", 1))
        ),
        0,
        0,
        0
      )
    );
  }

  @Test
  public void mergeMultiLineString() throws GeometryException {
    assertEquals(
      List.of(
        feature(1, newLineString(10, 10, 40, 40), Map.of("a", 1))
      ),
      FeatureMerge.mergeLineStrings(
        List.of(
          feature(1, newMultiLineString(
            newLineString(10, 10, 20, 20),
            newLineString(30, 30, 40, 40)
          ), Map.of("a", 1)),
          feature(2, newLineString(20, 20, 30, 30), Map.of("a", 1))
        ),
        0,
        0,
        0
      )
    );
  }

  @Test
  public void mergeLineStringIgnoreNonLineString() throws GeometryException {
    assertEquals(
      List.of(
        feature(3, newPoint(5, 5), Map.of("a", 1)),
        feature(4, rectangle(50, 60), Map.of("a", 1)),
        feature(1, newLineString(10, 10, 30, 30), Map.of("a", 1))
      ),
      FeatureMerge.mergeLineStrings(
        List.of(
          feature(1, newLineString(10, 10, 20, 20), Map.of("a", 1)),
          feature(2, newLineString(20, 20, 30, 30), Map.of("a", 1)),
          feature(3, newPoint(5, 5), Map.of("a", 1)),
          feature(4, rectangle(50, 60), Map.of("a", 1))
        ),
        0,
        0,
        0
      )
    );
  }

  @Test
  public void mergeLineStringRemoveDetailOutsideTile() throws GeometryException {
    assertEquals(
      List.of(
        feature(1, newMultiLineString(
          newLineString(
            10, 10,
            -10, 20,
            10, 30,
            -10, 40,
            -10, 50,
            10, 60,
            -10, 70
          ),
          newLineString(
            -10, 100,
            10, 100
          )
        ), Map.of("a", 1))
      ),
      FeatureMerge.mergeLineStrings(
        List.of(
          // one point goes out - dont clip
          feature(1, newLineString(10, 10, -10, 20), Map.of("a", 1)),
          feature(2, newLineString(-10, 20, 10, 30), Map.of("a", 1)),
          feature(3, newLineString(10, 30, -10, 40), Map.of("a", 1)),
          // two points goes out - dont clip
          feature(4, newLineString(-10, 40, -10, 50), Map.of("a", 1)),
          feature(5, newLineString(-10, 50, 10, 60), Map.of("a", 1)),
          feature(5, newLineString(10, 60, -10, 70), Map.of("a", 1)),
          // three points out - do clip
          feature(6, newLineString(-10, 70, -10, 80), Map.of("a", 1)),
          feature(7, newLineString(-10, 80, -11, 90), Map.of("a", 1)),
          feature(8, newLineString(-10, 90, -10, 100), Map.of("a", 1)),
          feature(9, newLineString(-10, 100, 10, 100), Map.of("a", 1))
        ),
        0,
        0,
        1
      )
    );
  }

  @Test
  public void mergeLineStringMinLength() throws GeometryException {
    assertEquals(
      List.of(
        feature(2, newLineString(20, 20, 20, 25), Map.of("a", 1))
      ),
      FeatureMerge.mergeLineStrings(
        List.of(
          // too short - omit entire feature
          feature(1, newLineString(10, 10, 10, 14), Map.of("b", 1)),

          // too short - omit from combined group
          feature(2, newLineString(20, 10, 20, 12), Map.of("a", 1)),
          feature(3, newLineString(20, 12, 20, 14), Map.of("a", 1)),

          // just long enough
          feature(4, newLineString(20, 20, 20, 24), Map.of("a", 1)),
          feature(5, newLineString(20, 24, 20, 25), Map.of("a", 1))
        ),
        5,
        0,
        0
      )
    );
  }

  /*
   * POLYGON MERGE TESTS
   */

  @Test
  public void mergePolygonEmptyList() throws GeometryException {
    assertEquivalentFeatures(
      List.of(),
      FeatureMerge.mergePolygons(
        List.of(),
        0,
        0,
        0
      )
    );
  }

  @Test
  public void dontMergeDisconnectedPolygons() throws GeometryException {
    assertEquivalentFeatures(
      List.of(
        feature(1, newMultiPolygon(
          rectangle(10, 20),
          rectangle(22, 10, 30, 20)
        ), Map.of("a", 1))
      ),
      FeatureMerge.mergePolygons(
        List.of(
          feature(1, rectangle(10, 20), Map.of("a", 1)),
          feature(2, rectangle(22, 10, 30, 20), Map.of("a", 1))
        ),
        0,
        0,
        0
      )
    );
  }

  @Test
  public void dontMergeConnectedPolygonsWithDifferentAttrs() throws GeometryException {
    assertEquivalentFeatures(
      List.of(
        feature(1, rectangle(10, 20), Map.of("a", 1)),
        feature(2, rectangle(20, 10, 30, 20), Map.of("b", 1))
      ),
      FeatureMerge.mergePolygons(
        List.of(
          feature(1, rectangle(10, 20), Map.of("a", 1)),
          feature(2, rectangle(20, 10, 30, 20), Map.of("b", 1))
        ),
        0,
        0,
        0
      )
    );
  }

  @Test
  public void mergeConnectedPolygonsWithSameAttrs() throws GeometryException {
    assertEquivalentFeatures(
      List.of(
        feature(1, rectangle(10, 10, 30, 20), Map.of("a", 1))
      ),
      FeatureMerge.mergePolygons(
        List.of(
          feature(1, rectangle(10, 20), Map.of("a", 1)),
          feature(2, rectangle(20, 10, 30, 20), Map.of("a", 1))
        ),
        0,
        0,
        1
      )
    );
  }

  @Test
  public void mergeMultiPolygons() throws GeometryException {
    assertEquivalentFeatures(
      List.of(
        feature(1, rectangle(10, 10, 40, 20), Map.of("a", 1))
      ),
      FeatureMerge.mergePolygons(
        List.of(
          feature(1, newMultiPolygon(
            rectangle(10, 20),
            rectangle(30, 10, 40, 20)
          ), Map.of("a", 1)),
          feature(2, rectangle(15, 10, 35, 20), Map.of("a", 1))
        ),
        0,
        0,
        1
      )
    );
  }

  @Test
  public void mergePolygonsIgnoreNonPolygons() throws GeometryException {
    assertEquivalentFeatures(
      List.of(
        feature(2, newLineString(20, 10, 30, 20), Map.of("a", 1)),
        feature(1, rectangle(10, 20), Map.of("a", 1))
      ),
      FeatureMerge.mergePolygons(
        List.of(
          feature(1, rectangle(10, 20), Map.of("a", 1)),
          feature(2, newLineString(20, 10, 30, 20), Map.of("a", 1))
        ),
        0,
        0,
        0
      )
    );
  }

  @Test
  public void mergePolygonsWithinMinDist() throws GeometryException {
    assertEquivalentFeatures(
      List.of(
        feature(1, rectangle(10, 10, 30, 20), Map.of("a", 1))
      ),
      FeatureMerge.mergePolygons(
        List.of(
          feature(1, rectangle(10, 20), Map.of("a", 1)),
          feature(2, rectangle(20.9, 10, 30, 20), Map.of("a", 1))
        ),
        0,
        1,
        1
      )
    );
  }

  @Test
  public void dontMergePolygonsAboveMinDist() throws GeometryException {
    assertEquivalentFeatures(
      List.of(
        feature(1, newMultiPolygon(
          rectangle(10, 20),
          rectangle(21.1, 10, 30, 20)
        ), Map.of("a", 1))
      ),
      FeatureMerge.mergePolygons(
        List.of(
          feature(1, rectangle(10, 20), Map.of("a", 1)),
          feature(2, rectangle(21.1, 10, 30, 20), Map.of("a", 1))
        ),
        0,
        1,
        1
      )
    );
  }

  @Test
  public void removePolygonsBelowMinSize() throws GeometryException {
    assertEquivalentFeatures(
      List.of(),
      FeatureMerge.mergePolygons(
        List.of(
          feature(1, rectangle(10, 20), Map.of("a", 1)),
          feature(2, rectangle(30, 10, 36, 20), Map.of("a", 1)),
          feature(3, rectangle(35, 10, 40, 20), Map.of("a", 1))
        ),
        101,
        0,
        0
      )
    );
  }

  @Test
  public void allowPolygonsAboveMinSize() throws GeometryException {
    assertEquivalentFeatures(
      List.of(
        feature(1, newMultiPolygon(
          rectangle(10, 20),
          rectangle(30, 10, 40, 20)
        ), Map.of("a", 1))
      ),
      FeatureMerge.mergePolygons(
        List.of(
          feature(1, rectangle(10, 20), Map.of("a", 1)),
          feature(2, rectangle(30, 10, 36, 20), Map.of("a", 1)),
          feature(3, rectangle(35, 10, 40, 20), Map.of("a", 1))
        ),
        99,
        0,
        1
      )
    );
  }

  private static void assertEquivalentFeatures(List<VectorTileEncoder.Feature> expected,
    List<VectorTileEncoder.Feature> actual) throws GeometryException {
    for (var feature : actual) {
      Geometry geom = feature.geometry().decode();
      TestUtils.validateGeometry(geom);
    }
    assertEquals(
      expected.stream().map(f -> f.copyWithNewGeometry(newPoint(0, 0))).toList(),
      actual.stream().map(f -> f.copyWithNewGeometry(newPoint(0, 0))).toList(),
      "comparison without geometries"
    );
    assertEquals(
      expected.stream().map(f -> new NormGeometry(silence(() -> f.geometry().decode()))).toList(),
      actual.stream().map(f -> new NormGeometry(silence(() -> f.geometry().decode()))).toList(),
      "geometry comparison"
    );
  }

  private interface SupplierThatThrows<T> {

    T get() throws Exception;
  }

  private static <T> T silence(SupplierThatThrows<T> fn) {
    try {
      return fn.get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @SafeVarargs
  private static IntObjectMap<IntArrayList> adjacencyListFromGroups(List<Integer>... groups) {
    IntObjectMap<IntArrayList> result = new GHIntObjectHashMap<>();
    for (List<Integer> group : groups) {
      for (int i = 0; i < group.size(); i++) {
        Integer a = group.get(i);
        for (int j = 0; j < i; j++) {
          Integer b = group.get(j);
          var aval = result.getOrDefault(a, new IntArrayList());
          aval.add(b);
          result.put(a, aval);

          var bval = result.getOrDefault(b, new IntArrayList());
          bval.add(a);
          result.put(b, bval);
        }
      }
    }
    return result;
  }

  @Test
  public void testExtractConnectedComponentsEmpty() {
    assertEquals(
      List.of(), FeatureMerge.extractConnectedComponents(new GHIntObjectHashMap<>(), 0)
    );
  }

  @Test
  public void testExtractConnectedComponentsOne() {
    assertEquals(
      List.of(
        IntArrayList.from(0)
      ), FeatureMerge.extractConnectedComponents(new GHIntObjectHashMap<>(), 1)
    );
  }

  @Test
  public void testExtractConnectedComponentsTwoDisconnected() {
    assertEquals(
      List.of(
        IntArrayList.from(0),
        IntArrayList.from(1)
      ), FeatureMerge.extractConnectedComponents(new GHIntObjectHashMap<>(), 2)
    );
  }

  @Test
  public void testExtractConnectedComponentsTwoConnected() {
    assertEquals(
      List.of(
        IntArrayList.from(0, 1)
      ), FeatureMerge.extractConnectedComponents(adjacencyListFromGroups(
        List.of(0, 1)
      ), 2)
    );
  }

  @Test
  public void testExtractConnectedComponents() {
    assertEquals(
      List.of(
        IntArrayList.from(0, 1, 2, 3),
        IntArrayList.from(4),
        IntArrayList.from(5, 6)
      ), FeatureMerge.extractConnectedComponents(adjacencyListFromGroups(
        List.of(0, 1, 2, 3),
        List.of(4),
        List.of(5, 6)
      ), 7)
    );
  }
}
