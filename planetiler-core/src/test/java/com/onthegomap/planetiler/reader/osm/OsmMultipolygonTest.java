/* ****************************************************************
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ****************************************************************/
package com.onthegomap.planetiler.reader.osm;

import static com.onthegomap.planetiler.TestUtils.*;
import static com.onthegomap.planetiler.reader.osm.OsmMultipolygon.appendToSkipFirst;
import static com.onthegomap.planetiler.reader.osm.OsmMultipolygon.connectPolygonSegments;
import static com.onthegomap.planetiler.reader.osm.OsmMultipolygon.prependToSkipLast;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.carrotsearch.hppc.LongArrayList;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.GeometryException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DynamicNode;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.Geometry;

/**
 * This class is ported to Java from https://github.com/omniscale/imposm3/blob/master/geom/multipolygon_test.go
 */
class OsmMultipolygonTest {

  private static LongArrayList longs(long... input) {
    return LongArrayList.from(input);
  }

  private DynamicTest testConnect(List<LongArrayList> input, List<LongArrayList> expectedOutput) {
    return DynamicTest.dynamicTest("connectPolygonSegments(" + input + ")", () -> {
      var expected = expectedOutput.stream().sorted(Comparator.comparing(Object::toString)).toList();
      var actual = connectPolygonSegments(input);
      actual.sort(Comparator.comparing(Object::toString));
      assertEquals(expected, actual);
    });
  }

  @Test
  void testConnectUtils() {
    assertEquals(longs(1, 2, 3, 4), appendToSkipFirst(longs(1, 2), longs(2, 3, 4)));
    assertEquals(longs(1, 2, 3, 4), prependToSkipLast(longs(3, 4), longs(1, 2, 3)));
  }

  @TestFactory
  List<DynamicNode> testConnectPolygonSegments() {
    return List.of(
      testConnect(List.of(), List.of()),
      testConnect(List.of(longs(1)), List.of()),
      testConnect(List.of(longs(1, 2)), List.of()),
      testConnect(List.of(longs(1, 2, 1)), List.of()),
      testConnect(List.of(longs(1, 2, 3, 1)), List.of(longs(1, 2, 3, 1))),
      testConnect(List.of(
        longs(1, 2),
        longs(3, 2),
        longs(3, 1)
      ), List.of(longs(1, 2, 3, 1))),
      testConnect(List.of(
        longs(10, 11, 10),
        longs(1, 2),
        longs(3, 2),
        longs(3, 1),
        longs(4, 5),
        longs(5, 6),
        longs(6, 4)
      ), List.of(
        longs(4, 5, 6, 4),
        longs(1, 2, 3, 1)
      )),
      testConnect(List.of(
        longs(1L, 2L),
        longs(3L, 4L),
        longs(4L, 5L),
        longs(5L, 6L),
        longs(6L, 7L),
        longs(3L, 2L),
        longs(7L, 1L)
      ), List.of(
        longs(1, 2, 3, 4, 5, 6, 7, 1)
      ))
    );
  }

  private long id = 1;

  private record Node(long id, double x, double y) {}

  private Node node(double x, double y) {
    return new Node(id++, x, y);
  }

  private void testBuildMultipolygon(List<List<Node>> ways, Geometry expected, boolean withOrdering)
    throws GeometryException {
    Map<Long, Coordinate> coords = new HashMap<>();
    List<LongArrayList> rings = new ArrayList<>();
    for (List<Node> way : ways) {
      LongArrayList ring = new LongArrayList();
      rings.add(ring);
      for (Node node : way) {
        ring.add(node.id);
        coords.put(node.id, new CoordinateXY(node.x, node.y));
      }
    }
    OsmReader.NodeLocationProvider nodeLocs = coords::get;
    Geometry actual = OsmMultipolygon.build(rings, nodeLocs, 0);
    assertSameNormalizedFeature(expected, actual);
    if (withOrdering) {
      assertEquals(expected.toString(), actual.toString());
    }
  }

  private void testBuildMultipolygon(List<List<Node>> ways, Geometry expected) throws GeometryException {
    testBuildMultipolygon(ways, expected, false);
  }

  @Test
  void testConnectSimplePolygon() throws GeometryException {
    var node1 = node(0.5, 0.5);
    var node2 = node(0.75, 0.5);
    var node3 = node(0.75, 0.75);
    testBuildMultipolygon(
      List.of(
        List.of(node1, node2),
        List.of(node2, node3),
        List.of(node1, node3)
      ),
      newPolygon(
        0.5, 0.5,
        0.75, 0.5,
        0.75, 0.75,
        0.5, 0.5
      )
    );
  }

  @Test
  void testConnectAlmostClosed() throws GeometryException {
    var node1 = node(0.5, 0.5);
    var node1a = node(0.5 + 1e-10, 0.5);
    var node2 = node(0.75, 0.5);
    var node3 = node(0.75, 0.75);
    testBuildMultipolygon(
      List.of(
        List.of(node1, node2),
        List.of(node2, node3),
        List.of(node1a, node3)
      ),
      newPolygon(
        0.5, 0.5,
        0.75, 0.5,
        0.75, 0.75,
        0.5, 0.5
      )
    );
  }

  @Test
  void testBuildMultipolygonFromGeometries() throws GeometryException {
    Geometry actual = OsmMultipolygon.build(List.of(
      newLineString(0.2, 0.2, 0.4, 0.2, 0.4, 0.4).getCoordinateSequence(),
      newLineString(0.4, 0.4, 0.2, 0.4, 0.2, 0.2).getCoordinateSequence()
    ));
    assertSameNormalizedFeature(rectangle(0.2, 0.4), actual);
  }

  @Test
  void testThrowWhenNoClosed() {
    var node1 = node(0.5, 0.5);
    var node1a = node(0.5 + 1e-1, 0.5);
    var node2 = node(0.75, 0.5);
    var node3 = node(0.75, 0.75);
    assertThrows(GeometryException.class, () -> testBuildMultipolygon(
      List.of(
        List.of(node1, node2),
        List.of(node2, node3),
        List.of(node1a, node3)
      ),
      GeoUtils.JTS_FACTORY.createGeometryCollection()
    ));
  }


  @Test
  void testIgnoreSingleNotClosed() throws GeometryException {
    testBuildMultipolygon(
      List.of(
        rectangleNodes(0, 10),
        List.of(
          node(20, 20),
          node(30, 20),
          node(30, 30),
          node(20, 30)
        // not closed
        )
      ),
      rectangle(0, 10)
    );
  }
  // tests from https://github.com/omniscale/imposm3/blob/master/geom/multipolygon_test.go below

  @Test
  void testSimplePolygonWithHole() throws GeometryException {
    testBuildMultipolygon(
      List.of(
        rectangleNodes(0, 10),
        rectangleNodes(2, 8)
      ),
      newPolygon(
        rectangleCoordList(0, 10),
        List.of(rectangleCoordList(2, 8))
      )
    );
  }

  @Test
  void testSimplePolygonOrdering() throws GeometryException {
    testBuildMultipolygon(
      List.of(
        rectangleNodes(8, 10),
        rectangleNodes(0, 7)
      ),
      newMultiPolygon(
        rectangle(0, 7),
        rectangle(8, 10)
      ),
      true
    );
  }

  @Test
  void testSimplePolygonWithMultipleHoles() throws GeometryException {
    testBuildMultipolygon(
      List.of(
        rectangleNodes(0, 10),
        rectangleNodes(1, 2),
        rectangleNodes(3, 4)
      ),
      newPolygon(
        rectangleCoordList(0, 10),
        List.of(rectangleCoordList(1, 2), rectangleCoordList(3, 4))
      )
    );
  }

  public List<Node> rectangleNodes(double xMin, double yMin, double xMax, double yMax) {
    var startEnd = node(xMin, yMin);
    return List.of(startEnd, node(xMax, yMin), node(xMax, yMax), node(xMin, yMax), startEnd);
  }

  public List<Node> rectangleNodes(double min, double max) {
    return rectangleNodes(min, min, max, max);
  }

  @Test
  void testMultiPolygonWithNestedHoles() throws GeometryException {
    testBuildMultipolygon(
      List.of(
        rectangleNodes(0, 10),
        rectangleNodes(1, 9),
        rectangleNodes(2, 8),
        rectangleNodes(3, 7),
        rectangleNodes(4, 6)
      ),
      newMultiPolygon(
        newPolygon(
          rectangleCoordList(0, 10),
          List.of(
            rectangleCoordList(1, 9)
          )
        ),
        newPolygon(
          rectangleCoordList(2, 8),
          List.of(
            rectangleCoordList(3, 7)
          )
        ),
        rectangle(4, 6)
      )
    );
  }

  @Test
  void testTouchingPolygonsWithHole() throws GeometryException {
    testBuildMultipolygon(
      List.of(
        rectangleNodes(0, 10),
        rectangleNodes(10, 0, 30, 10),
        rectangleNodes(2, 8)
      ),
      newMultiPolygon(
        newPolygon(
          rectangleCoordList(0, 10),
          List.of(
            rectangleCoordList(2, 8)
          )
        ),
        rectangle(10, 0, 30, 10)
      )
    );
  }

  @Test
  void testBrokenPolygonSelfIntersect1() throws GeometryException {
    Node startEnd = node(0, 0);
    testBuildMultipolygon(
      List.of(
        List.of(
          startEnd,
          node(0, 10),
          node(10, 10),
          node(10, 0),
          node(20, 0),
          node(20, 10),
          node(30, 10),
          node(30, 0),
          startEnd
        ),
        rectangleNodes(2, 8)
      ),
      newPolygon(
        newCoordinateList(
          0, 0,
          0, 10,
          10, 10,
          10, 0,
          20, 0,
          20, 10,
          30, 10,
          30, 0,
          0, 0
        ),
        List.of(
          rectangleCoordList(2, 8)
        )
      )
    );
  }

  @Test
  void testBrokenPolygonSelfIntersect2() throws GeometryException {
    Node startEnd = node(10, 0);
    testBuildMultipolygon(
      List.of(
        List.of(
          startEnd,
          node(10, 0),
          node(0, 0),
          node(0, 10),
          node(10, 10),
          node(10, 0),
          node(20, 0),
          node(20, 10),
          node(30, 10),
          node(30, 0),
          startEnd
        ),
        rectangleNodes(2, 8)
      ),
      newPolygon(
        newCoordinateList(
          10, 0,
          10, 0,
          0, 0,
          0, 10,
          10, 10,
          10, 0,
          20, 0,
          20, 10,
          30, 10,
          30, 0,
          10, 0
        ),
        List.of(
          rectangleCoordList(2, 8)
        )
      )
    );
  }

  @Test
  void testBrokenPolygonSelfIntersectTriangleSmallOverlap() throws GeometryException {
    Node startEnd = node(0, 0);
    testBuildMultipolygon(
      List.of(
        List.of(
          startEnd,
          node(0, 100),
          node(100, 50 - 0.00001),
          node(100, 50 + 0.00001),
          startEnd
        ),
        rectangleNodes(10, 45, 20, 55)
      ),
      newPolygon(
        newCoordinateList(
          0, 0,
          0, 100,
          100, 50 - 0.00001,
          100, 50 + 0.00001,
          0, 0
        ),
        List.of(
          rectangleCoordList(10, 45, 20, 55)
        )
      )
    );
  }

  @Test
  void testBrokenPolygonSelfIntersectTriangleLargeOverlap() throws GeometryException {
    Node startEnd = node(0, 0);
    testBuildMultipolygon(
      List.of(
        List.of(
          startEnd,
          node(0, 100),
          node(100, 50 - 1),
          node(100, 50 + 1),
          startEnd
        ),
        rectangleNodes(10, 45, 20, 55)
      ),
      newPolygon(
        newCoordinateList(
          0, 0,
          0, 100,
          100, 50 - 1,
          100, 50 + 1,
          0, 0
        ),
        List.of(
          rectangleCoordList(10, 45, 20, 55)
        )
      )
    );
  }
}
