package com.onthegomap.planetiler;

import static com.onthegomap.planetiler.TestUtils.*;
import static com.onthegomap.planetiler.util.Gzip.gunzip;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.IntObjectMap;
import com.onthegomap.planetiler.collection.Hppc;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.geo.GeometryPipeline;
import com.onthegomap.planetiler.geo.GeometryType;
import com.onthegomap.planetiler.mbtiles.Mbtiles;
import com.onthegomap.planetiler.stats.Stats;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class FeatureMergeTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureMergeTest.class);

  private static VectorTile.Feature feature(long id, Geometry geom, Map<String, Object> attrs) {
    return new VectorTile.Feature(
      "layer",
      id,
      VectorTile.encodeGeometry(geom),
      attrs
    );
  }

  @Test
  void mergeMergeZeroLineStrings() {
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
  void mergeMergeOneLineStrings() {
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
  void dontMergeDisconnectedLineStrings() {
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
  void dontMergeConnectedLineStringsDifferentAttr() {
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
  void mergeConnectedLineStringsSameAttrs() {
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
  void simplifyLineStringIfToleranceIsSet() {
    // does not resimplify by default
    assertEquals(
      List.of(
        feature(1, newLineString(10, 10, 20, 20, 30, 30), Map.of("a", 1))
      ),
      FeatureMerge.mergeLineStrings(
        List.of(
          feature(1, newLineString(10, 10, 20, 20, 30, 30), Map.of("a", 1))
        ),
        0,
        1,
        0
      )
    );
    // but does resimplify when resimplify=true
    assertEquals(
      List.of(
        feature(1, newLineString(10, 10, 30, 30), Map.of("a", 1))
      ),
      FeatureMerge.mergeLineStrings(
        List.of(
          feature(1, newLineString(10, 10, 20, 20, 30, 30), Map.of("a", 1))
        ),
        0,
        1,
        0,
        true
      )
    );
    // but doesn't resimplify if the tolerance is negative even when resimplify=true
    assertEquals(
      List.of(
        feature(1, newLineString(10, 10, 20, 20, 30, 30), Map.of("a", 1))
      ),
      FeatureMerge.mergeLineStrings(
        List.of(
          feature(1, newLineString(10, 10, 20, 20, 30, 30), Map.of("a", 1))
        ),
        0,
        -1,
        0,
        true
      )
    );
  }

  @Test
  void mergeMultiLineString() {
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
  void mergeLineStringIgnoreNonLineString() {
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
  void mergeLineStringRemoveDetailOutsideTile() {
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
          // one point goes out - don't clip
          feature(1, newLineString(10, 10, -10, 20), Map.of("a", 1)),
          feature(2, newLineString(-10, 20, 10, 30), Map.of("a", 1)),
          feature(3, newLineString(10, 30, -10, 40), Map.of("a", 1)),
          // two points goes out - don't clip
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
  void mergeLineStringMinLength() {
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
  void mergePolygonEmptyList() throws GeometryException {
    assertEquivalentFeatures(
      List.of(),
      FeatureMerge.mergeNearbyPolygons(
        List.of(),
        0,
        0,
        0,
        0
      )
    );
  }

  @Test
  void dontMergeDisconnectedPolygons() throws GeometryException {
    assertEquivalentFeatures(
      List.of(
        feature(1, newMultiPolygon(
          rectangle(10, 20),
          rectangle(22, 10, 30, 20)
        ), Map.of("a", 1))
      ),
      FeatureMerge.mergeNearbyPolygons(
        List.of(
          feature(1, rectangle(10, 20), Map.of("a", 1)),
          feature(2, rectangle(22, 10, 30, 20), Map.of("a", 1))
        ),
        0,
        0,
        0,
        0
      )
    );
  }

  @Test
  void dontMergeConnectedPolygonsWithDifferentAttrs() throws GeometryException {
    assertEquivalentFeatures(
      List.of(
        feature(1, rectangle(10, 20), Map.of("a", 1)),
        feature(2, rectangle(20, 10, 30, 20), Map.of("b", 1))
      ),
      FeatureMerge.mergeNearbyPolygons(
        List.of(
          feature(1, rectangle(10, 20), Map.of("a", 1)),
          feature(2, rectangle(20, 10, 30, 20), Map.of("b", 1))
        ),
        0,
        0,
        0,
        0
      )
    );
  }

  @Test
  void mergeConnectedPolygonsWithSameAttrs() throws GeometryException {
    assertEquivalentFeatures(
      List.of(
        feature(1, rectangle(10, 10, 30, 20), Map.of("a", 1))
      ),
      FeatureMerge.mergeNearbyPolygons(
        List.of(
          feature(1, rectangle(10, 20), Map.of("a", 1)),
          feature(2, rectangle(20, 10, 30, 20), Map.of("a", 1))
        ),
        0,
        0,
        0,
        1
      )
    );
  }

  @Test
  void geometryPipelineWhenMergingOverlappingPolygons() throws GeometryException {
    List<VectorTile.Feature> features = List.of(
      feature(1, rectangle(10, 10, 20, 19), Map.of("a", 1)),
      feature(2, rectangle(11, 10, 20, 20), Map.of("a", 1))
    );
    assertEquivalentFeatures(
      List.of(
        feature(1, newPolygon(
          10, 10,
          20, 10,
          20, 20,
          11, 20,
          // remove this point due to simplification: 11, 19,
          10, 19,
          10, 10
        ), Map.of("a", 1))
      ),
      FeatureMerge.mergeNearbyPolygons(
        features,
        0,
        0,
        0,
        1,
        Stats.inMemory(),
        GeometryPipeline.simplifyVW(1)
      )
    );
  }

  @Test
  void geometryPipelineAppliedWhenMergingSinglePolygon() throws GeometryException {
    List<VectorTile.Feature> features = List.of(
      feature(1, newPolygon(
        10, 10,
        20, 10,
        20, 20,
        11, 20,
        11, 19,
        10, 19,
        10, 10), Map.of("a", 1))
    );
    assertEquivalentFeatures(
      List.of(
        feature(1, newPolygon(
          10, 10,
          20, 10,
          20, 20,
          11, 20,
          // remove this point due to simplification: 11, 19,
          10, 19,
          10, 10
        ), Map.of("a", 1))
      ),
      FeatureMerge.mergeNearbyPolygons(
        features,
        0,
        0,
        0,
        1,
        Stats.inMemory(),
        GeometryPipeline.simplifyVW(1)
      )
    );
  }


  @Test
  void mergeMultiPolygons() throws GeometryException {
    assertEquivalentFeatures(
      List.of(
        feature(1, rectangle(10, 10, 40, 20), Map.of("a", 1))
      ),
      FeatureMerge.mergeNearbyPolygons(
        List.of(
          feature(1, newMultiPolygon(
            rectangle(10, 20),
            rectangle(30, 10, 40, 20)
          ), Map.of("a", 1)),
          feature(2, rectangle(15, 10, 35, 20), Map.of("a", 1))
        ),
        0,
        0,
        0,
        1
      )
    );
  }

  @Test
  void mergePolygonsIgnoreNonPolygons() throws GeometryException {
    assertEquivalentFeatures(
      List.of(
        feature(2, newLineString(20, 10, 30, 20), Map.of("a", 1)),
        feature(1, rectangle(10, 20), Map.of("a", 1))
      ),
      FeatureMerge.mergeNearbyPolygons(
        List.of(
          feature(1, rectangle(10, 20), Map.of("a", 1)),
          feature(2, newLineString(20, 10, 30, 20), Map.of("a", 1))
        ),
        0,
        0,
        0,
        0
      )
    );
  }

  @Test
  void mergePolygonsWithinMinDist() throws GeometryException {
    assertEquivalentFeatures(
      List.of(
        feature(1, rectangle(10, 10, 30, 20), Map.of("a", 1))
      ),
      FeatureMerge.mergeNearbyPolygons(
        List.of(
          feature(1, rectangle(10, 20), Map.of("a", 1)),
          feature(2, rectangle(20.9, 10, 30, 20), Map.of("a", 1))
        ),
        0,
        0,
        1,
        1
      )
    );
  }

  @Test
  void mergePolygonsInsideEachother() throws GeometryException {
    assertEquivalentFeatures(
      List.of(
        feature(1, rectangle(10, 40), Map.of("a", 1))
      ),
      FeatureMerge.mergeNearbyPolygons(
        List.of(
          feature(1, rectangle(10, 40), Map.of("a", 1)),
          feature(2, rectangle(20, 30), Map.of("a", 1))
        ),
        0,
        0,
        1,
        1
      )
    );
  }

  @Test
  void dontMergePolygonsAboveMinDist() throws GeometryException {
    assertEquivalentFeatures(
      List.of(
        feature(1, newMultiPolygon(
          rectangle(10, 20),
          rectangle(21.1, 10, 30, 20)
        ), Map.of("a", 1))
      ),
      FeatureMerge.mergeNearbyPolygons(
        List.of(
          feature(1, rectangle(10, 20), Map.of("a", 1)),
          feature(2, rectangle(21.1, 10, 30, 20), Map.of("a", 1))
        ),
        0,
        0,
        1,
        1
      )
    );
  }

  @Test
  void removePolygonsBelowMinSize() throws GeometryException {
    assertEquivalentFeatures(
      List.of(),
      FeatureMerge.mergeNearbyPolygons(
        List.of(
          feature(1, rectangle(10, 20), Map.of("a", 1)),
          feature(2, rectangle(30, 10, 36, 20), Map.of("a", 1)),
          feature(3, rectangle(35, 10, 40, 20), Map.of("a", 1))
        ),
        101,
        0,
        0,
        0
      )
    );
  }

  @Test
  void allowPolygonsAboveMinSize() throws GeometryException {
    assertEquivalentFeatures(
      List.of(
        feature(1, newMultiPolygon(
          rectangle(10, 20),
          rectangle(30, 10, 40, 20)
        ), Map.of("a", 1))
      ),
      FeatureMerge.mergeNearbyPolygons(
        List.of(
          feature(1, rectangle(10, 20), Map.of("a", 1)),
          feature(2, rectangle(30, 10, 36, 20), Map.of("a", 1)),
          feature(3, rectangle(35, 10, 40, 20), Map.of("a", 1))
        ),
        99,
        0,
        0,
        1
      )
    );
  }

  private static void assertEquivalentFeatures(List<VectorTile.Feature> expected,
    List<VectorTile.Feature> actual) throws GeometryException {
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

  private static void assertTopologicallyEquivalentFeatures(List<VectorTile.Feature> expected,
    List<VectorTile.Feature> actual) throws GeometryException {
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
      expected.stream().map(f -> new TopoGeometry(silence(() -> f.geometry().decode()))).toList(),
      actual.stream().map(f -> new TopoGeometry(silence(() -> f.geometry().decode()))).toList(),
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
    IntObjectMap<IntArrayList> result = Hppc.newIntObjectHashMap();
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
  void testExtractConnectedComponentsEmpty() {
    assertEquals(
      List.of(), FeatureMerge.extractConnectedComponents(Hppc.newIntObjectHashMap(), 0)
    );
  }

  @Test
  void testExtractConnectedComponentsOne() {
    assertEquals(
      List.of(
        IntArrayList.from(0)
      ), FeatureMerge.extractConnectedComponents(Hppc.newIntObjectHashMap(), 1)
    );
  }

  @Test
  void testExtractConnectedComponentsTwoDisconnected() {
    assertEquals(
      List.of(
        IntArrayList.from(0),
        IntArrayList.from(1)
      ), FeatureMerge.extractConnectedComponents(Hppc.newIntObjectHashMap(), 2)
    );
  }

  @Test
  void testExtractConnectedComponentsTwoConnected() {
    assertEquals(
      List.of(
        IntArrayList.from(0, 1)
      ), FeatureMerge.extractConnectedComponents(adjacencyListFromGroups(
        List.of(0, 1)
      ), 2)
    );
  }

  @Test
  void testExtractConnectedComponents() {
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

  @Slow
  @ParameterizedTest
  @CsvSource({
    "bostonbuildings.mbtiles, 2477, 3028, 13, 1141",
    "bostonbuildings.mbtiles, 2481, 3026, 13, 949",
    "bostonbuildings.mbtiles, 2479, 3028, 13, 1074",
    "jakartabuildings.mbtiles, 6527, 4240, 13, 410"
  })
  void testMergeManyPolygons__TAKES_A_MINUTE_OR_TWO(String file, int x, int y, int z, int expected)
    throws IOException, GeometryException {
    LOGGER.warn("Testing complex polygon merging for {} {}/{}/{} ...", file, z, x, y);
    try (var db = Mbtiles.newReadOnlyDatabase(TestUtils.pathToResource(file))) {
      byte[] tileData = db.getTile(x, y, z);
      byte[] gunzipped = gunzip(tileData);
      List<VectorTile.Feature> features = VectorTile.decode(gunzipped);
      List<VectorTile.Feature> merged = FeatureMerge.mergeNearbyPolygons(
        features,
        4,
        0,
        0.5,
        0.5
      );
      int total = 0;
      for (var feature : merged) {
        Geometry geometry = feature.geometry().decode();
        total += geometry.getNumGeometries();
        TestUtils.validateGeometry(geometry);
      }
      assertEquals(expected, total);
    }
  }

  @Test
  void mergeMultiPolygon() throws GeometryException {
    var innerRing = rectangleCoordList(12, 18);
    Collections.reverse(innerRing);
    assertTopologicallyEquivalentFeatures(
      List.of(
        feature(1, newPolygon(rectangleCoordList(10, 22), List.of(innerRing)), Map.of("a", 1))
      ),
      FeatureMerge.mergeNearbyPolygons(
        List.of(
          feature(1, newPolygon(rectangleCoordList(10, 20), List.of(innerRing)), Map.of("a", 1)),
          feature(1, rectangle(20, 10, 22, 22), Map.of("a", 1)),
          feature(1, rectangle(10, 20, 22, 22), Map.of("a", 1))
        ),
        0,
        0,
        0,
        0
      )
    );
  }

  @Test
  void mergeMultiPolygonExcludeSmallInnerRings() throws GeometryException {
    var innerRing = rectangleCoordList(12, 12.99);
    Collections.reverse(innerRing);
    assertTopologicallyEquivalentFeatures(
      List.of(
        feature(1, rectangle(10, 22), Map.of("a", 1))
      ),
      FeatureMerge.mergeNearbyPolygons(
        List.of(
          feature(1, newPolygon(rectangleCoordList(10, 20), List.of(innerRing)), Map.of("a", 1)),
          feature(1, rectangle(20, 10, 22, 22), Map.of("a", 1)),
          feature(1, rectangle(10, 20, 22, 22), Map.of("a", 1))
        ),
        1,
        1,
        0,
        0
      )
    );
  }

  @Test
  void mergeMultipoints() throws GeometryException {
    testMultigeometryMerger(
      i -> newPoint(i, 2 * i),
      items -> newMultiPoint(items.toArray(Point[]::new)),
      rectangle(0, 1),
      FeatureMerge::mergeMultiPoint
    );
  }

  @Test
  void mergeMultipolygons() throws GeometryException {
    testMultigeometryMerger(
      i -> rectangle(i, i + 1),
      items -> newMultiPolygon(items.toArray(Polygon[]::new)),
      newPoint(0, 0),
      FeatureMerge::mergeMultiPolygon
    );
  }

  @Test
  void mergeMultiline() throws GeometryException {
    testMultigeometryMerger(
      i -> newLineString(i, i + 1, i + 2, i + 3),
      items -> newMultiLineString(items.toArray(LineString[]::new)),
      newPoint(0, 0),
      FeatureMerge::mergeMultiLineString
    );
  }

  <S extends Geometry, M extends GeometryCollection> void testMultigeometryMerger(
    IntFunction<S> generateGeometry,
    Function<List<S>, M> combineJTS,
    Geometry otherGeometry,
    UnaryOperator<List<VectorTile.Feature>> merge
  ) throws GeometryException {
    var geom1 = generateGeometry.apply(1);
    var geom2 = generateGeometry.apply(2);
    var geom3 = generateGeometry.apply(3);
    var geom4 = generateGeometry.apply(4);
    var geom5 = generateGeometry.apply(5);

    assertTopologicallyEquivalentFeatures(
      List.of(),
      merge.apply(List.of())
    );

    assertTopologicallyEquivalentFeatures(
      List.of(
        feature(1, geom1, Map.of("a", 1))
      ),
      merge.apply(
        List.of(
          feature(1, geom1, Map.of("a", 1))
        )
      )
    );

    assertTopologicallyEquivalentFeatures(
      List.of(
        feature(4, otherGeometry, Map.of("a", 1)),
        feature(1, combineJTS.apply(List.of(geom1, geom2, geom3, geom4)), Map.of("a", 1)),
        feature(3, geom5, Map.of("a", 2))
      ),
      merge.apply(
        List.of(
          feature(1, combineJTS.apply(List.of(geom1, geom2)), Map.of("a", 1)),
          feature(2, combineJTS.apply(List.of(geom3, geom4)), Map.of("a", 1)),
          feature(3, geom5, Map.of("a", 2)),
          feature(4, otherGeometry, Map.of("a", 1)),
          new VectorTile.Feature("layer", 5, new VectorTile.VectorGeometry(new int[0], GeometryType.typeOf(geom1), 0),
            Map.of("a", 1))
        )
      )
    );
  }

  @Test
  void removePointsOutsideBufferEmpty() {
    assertEquals(
      List.of(),
      FeatureMerge.removePointsOutsideBuffer(List.of(), 4d)
    );
  }

  @Test
  void removePointsOutsideBufferSinglePoints() throws GeometryException {
    assertEquals(
      List.of(),
      FeatureMerge.removePointsOutsideBuffer(List.of(), 4d)
    );
    assertTopologicallyEquivalentFeatures(
      List.of(
        feature(1, newPoint(0, 0), Map.of()),
        feature(1, newPoint(256, 256), Map.of()),
        feature(1, newPoint(-4, -4), Map.of()),
        feature(1, newPoint(-4, 260), Map.of()),
        feature(1, newPoint(260, -4), Map.of()),
        feature(1, newPoint(260, 260), Map.of())
      ),
      FeatureMerge.removePointsOutsideBuffer(
        List.of(
          feature(1, newPoint(0, 0), Map.of()),
          feature(1, newPoint(256, 256), Map.of()),
          feature(1, newPoint(-4, -4), Map.of()),
          feature(1, newPoint(-4, 260), Map.of()),
          feature(1, newPoint(260, -4), Map.of()),
          feature(1, newPoint(260, 260), Map.of()),
          feature(1, newPoint(-5, -5), Map.of()),
          feature(1, newPoint(-5, 261), Map.of()),
          feature(1, newPoint(261, -5), Map.of()),
          feature(1, newPoint(261, 261), Map.of())
        ),
        4d
      )
    );
  }

  @Test
  void removePointsOutsideBufferMultiPoints() throws GeometryException {
    assertEquals(
      List.of(),
      FeatureMerge.removePointsOutsideBuffer(List.of(), 4d)
    );
    assertTopologicallyEquivalentFeatures(
      List.of(
        feature(1, newMultiPoint(
          newPoint(0, 0),
          newPoint(256, 256),
          newPoint(-4, -4),
          newPoint(-4, 260),
          newPoint(260, -4),
          newPoint(260, 260)
        ), Map.of())
      ),
      FeatureMerge.removePointsOutsideBuffer(
        List.of(
          feature(1, newMultiPoint(
            newPoint(0, 0),
            newPoint(256, 256),
            newPoint(-4, -4),
            newPoint(-4, 260),
            newPoint(260, -4),
            newPoint(260, 260),
            newPoint(-5, -5),
            newPoint(-5, 261),
            newPoint(261, -5),
            newPoint(261, 261)
          ), Map.of()),
          feature(1, newMultiPoint(
            newPoint(-5, -5),
            newPoint(-5, 261),
            newPoint(261, -5),
            newPoint(261, 261)
          ), Map.of())
        ),
        4d
      )
    );
  }

  @ParameterizedTest
  @ValueSource(strings = {
    "/issue_700/exception_1.wkb",
    "/issue_700/exception_2.wkb",
    "/issue_700/exception_3.wkb",
    "/issue_700/exception_4.wkb",
    "/issue_700/exception_5.wkb",
    "/issue_700/exception_6.wkb",
    "/issue_700/exception_7.wkb",
    "/issue_700/exception_8.wkb",
    "/issue_700/exception_9.wkb",
  })
  void testIssue700BufferUnionUnbufferFailure(String path) throws IOException, ParseException {
    try (var is = getClass().getResource(path).openStream()) {
      GeometryCollection collection = (GeometryCollection) new WKBReader().read(is.readAllBytes());
      List<Geometry> geometries = new ArrayList<>();
      for (int i = 0; i < collection.getNumGeometries(); i++) {
        geometries.add(collection.getGeometryN(i));
      }
      FeatureMerge.bufferUnionUnbuffer(0.5, geometries, Stats.inMemory());
    }
  }

  @Test
  void mergeFillPolygonsNormalizes() throws GeometryException {
    assertEquals(
      List.of(
        rectangle(-2, 258)
      ),
      FeatureMerge.mergeNearbyPolygons(
        List.of(
          feature(1, rectangle(-2, -2, 200, 258), Map.of()),
          feature(2, rectangle(180, -2, 258, 258), Map.of())
        ),
        0,
        0,
        0,
        0
      ).stream().map(feature -> {
        try {
          return feature.geometry().decode();
        } catch (GeometryException e) {
          return fail(e);
        }
      }).toList()
    );
  }

  @Test
  void mergeNormalizeOuterRing() throws GeometryException {
    var result = FeatureMerge.mergeNearbyPolygons(
      List.of(
        feature(1, rectangle(-2, -2, 10, 258), Map.of()),
        feature(1, rectangle(-2, -2, 258, 10), Map.of()),
        feature(1, rectangle(246, -2, 258, 258), Map.of()),
        feature(1, rectangle(-2, 246, 258, 258), Map.of())
      ),
      0,
      0,
      0,
      0
    );
    Polygon poly = (Polygon) result.getFirst().geometry().decode();
    assertEquals(rectangle(-2, 258).getExteriorRing(), poly.getExteriorRing());
    assertEquals(1, poly.getNumInteriorRing());
    assertTopologicallyEquivalentFeature(rectangle(10, 246).getExteriorRing().reverse(), poly.getInteriorRingN(0));
  }

  @Test
  void mergeFillPolygonsDoesNotNormalizeIrregularFill() throws GeometryException {
    assertEquivalentFeatures(
      List.of(
        feature(1, newPolygon(
          -2, -2,
          200, -2,
          200, -1,
          258, -1,
          258, 257,
          200, 257,
          200, 258,
          -2, 258,
          -2, -2
        ), Map.of())
      ),
      FeatureMerge.mergeNearbyPolygons(
        List.of(
          feature(1, rectangle(-2, -2, 200, 258), Map.of()),
          feature(2, rectangle(180, -1, 258, 257), Map.of())
        ),
        0,
        0,
        0,
        0
      )
    );
  }

  @ParameterizedTest
  @CsvSource({
    "0, 0, 0",
    "0, -1, 0",
    "0, -1, -1",
    "0, 0, -1",
  })
  void mergeLineStringZeroMinLength(double minLength, double minTolerance, double buffer) throws GeometryException {
    var input = feature(1, newLineString(10, 10, 10.25, 10, 20, 10), Map.of());
    var actual = FeatureMerge.mergeLineStrings(
      List.of(
        feature(1, newLineString(10, 10, 10.25, 10, 20, 10), Map.of())
      ),
      minLength,
      minTolerance,
      buffer
    );
    var actualSingle = actual.getFirst();
    assertEquals(input.geometry().decode(), actualSingle.geometry().decode());
    assertEquals(List.of(input), actual);
  }
}
