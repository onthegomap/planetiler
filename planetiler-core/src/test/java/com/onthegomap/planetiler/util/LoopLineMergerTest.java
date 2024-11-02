package com.onthegomap.planetiler.util;
import static com.onthegomap.planetiler.TestUtils.newLineString;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;

class LoopLineMergerTest {

  @Test
  void testRoundCoordinate() {
    // rounds coordinates to fraction of 1 / 16 == 0.0625
    Coordinate coordinate = new Coordinate(0.05, 0.07);
    assertEquals(0.0625, LoopLineMerger.roundCoordinate(coordinate).getX());
    assertEquals(0.0625, LoopLineMerger.roundCoordinate(coordinate).getY());
  }

  @Test
  void testSplit() {
    // splits linestrings into linear segments
    assertEquals(
      List.of(
        newLineString(10, 10, 20, 20),
        newLineString(20, 20, 30, 30)
      ),
      LoopLineMerger.split(newLineString(10, 10, 20, 20, 30, 30))
    );

    // does not keep zero length linestrings
    assertEquals(0, LoopLineMerger.split(newLineString(10, 10, 10, 10)).size());

    // rounds coordinates to 1/16 grid
    assertEquals(
      List.of(
        newLineString(10.0625, 10.0625, 20, 20),
        newLineString(20, 20, 30, 30)
      ),
      LoopLineMerger.split(newLineString(10.062343634, 10.062343634, 20, 20, 30, 30))
    );
  }

  @Test
  void testConcat() {
    // concatenates forward A followed by forward B
    assertEquals(
      newLineString(10, 10, 20, 20, 30, 30),
      LoopLineMerger.concat(
        newLineString(10, 10, 20, 20),
        newLineString(20, 20, 30, 30)
      )
    );

    // concatenates forward B followed by forward A
    assertEquals(
      newLineString(10, 10, 20, 20, 30, 30),
      LoopLineMerger.concat(
        newLineString(20, 20, 30, 30),
        newLineString(10, 10, 20, 20)
      )
    );

    // concatenates A and B with same start point to backward A forward B
    assertEquals(
      newLineString(10, 10, 20, 20, 30, 30),
      LoopLineMerger.concat(
        newLineString(20, 20, 10, 10),
        newLineString(20, 20, 30, 30)
      )
    );

    // concatenates A and B with same end point to forward A backward B
    assertEquals(
      newLineString(10, 10, 20, 20, 30, 30),
      LoopLineMerger.concat(
        newLineString(10, 10, 20, 20),
        newLineString(30, 30, 20, 20)
      )
    );

    // TODO: handle and test the case when A/B do not share end/start points
  }
}
