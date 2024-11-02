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

  @Test
  void testMerge() {

    // merges two touching linestrings
    var merger = new LoopLineMerger();
    merger.add(newLineString(10, 10, 20, 20));
    merger.add(newLineString(20, 20, 30, 30));
    assertEquals(
      List.of(newLineString(10, 10, 20, 20, 30, 30)),
      merger.getMergedLineStrings(-1, -1)
    );

    // keeps two separate linestrings separate
    merger = new LoopLineMerger();
    merger.add(newLineString(10, 10, 20, 20));
    merger.add(newLineString(30, 30, 40, 40));
    assertEquals(
      List.of(
        newLineString(10, 10, 20, 20),
        newLineString(30, 30, 40, 40)
      ),
      merger.getMergedLineStrings(-1, -1)
    );

    // does not overcount already added linestrings
    merger = new LoopLineMerger();
    merger.add(newLineString(10, 10, 20, 20));
    merger.add(newLineString(20, 20, 30, 30));
    merger.add(newLineString(20, 20, 30, 30));
    assertEquals(
      List.of(newLineString(10, 10, 20, 20, 30, 30)),
      merger.getMergedLineStrings(-1, -1)
    );

    // splits linestrings into linear segments before merging
    merger = new LoopLineMerger();
    merger.add(newLineString(10, 10, 20, 20, 30, 30));
    merger.add(newLineString(20, 20, 30, 30, 40, 40));
    assertEquals(
      List.of(newLineString(10, 10, 20, 20, 30, 30, 40, 40)),
      merger.getMergedLineStrings(-1, -1)
    );

    // rounds coordinates to 1/16 before merging
    merger = new LoopLineMerger();
    merger.add(newLineString(10.00043983098, 10, 20, 20));
    merger.add(newLineString(20, 20, 30, 30));
    assertEquals(
      List.of(newLineString(10, 10, 20, 20, 30, 30)),
      merger.getMergedLineStrings(-1, -1)
    );

    // removes small loops, keeps shortes path
    merger = new LoopLineMerger();
    /**
     *              10  20 30 40
     *        10     o--o--o
     *                   \ |
     *                    \|
     *        20           o--o          
     */
    merger.add(newLineString(
      10, 10,
      20, 10,
      30, 10,
      30, 20,
      40, 20
    ));
    merger.add(newLineString(
      20, 10,
      30, 20
    ));
    assertEquals(
      List.of(
        newLineString(
          10, 10,
          20, 10,
          30, 20,
          40, 20
        )
      ),
      merger.getMergedLineStrings(-1, 100)
    );

    // does not remove large loops
    merger = new LoopLineMerger();
    /**
     *              10 20 30  40
     *        10     o--o--o
     *                   \ |
     *                    \|
     *        20           o--o          
     */
    merger.add(newLineString(
      10, 10,
      20, 10,
      30, 10,
      30, 20,
      40, 20
    ));
    merger.add(newLineString(
      20, 10,
      30, 20
    ));
    assertEquals(
      List.of(
        newLineString(
          30, 20,
          40, 20
        ),
        newLineString(
          20, 10,
          30, 20
        ),
        newLineString(
          20, 10,
          30, 10,
          30, 20
        ),
        newLineString(
          10, 10,
          20, 10
        )
      ),
      merger.getMergedLineStrings(-1, 0.001)
    );

    // removes linestrings that are too short after merging
    merger = new LoopLineMerger();
    merger.add(newLineString(10, 10, 11, 11));
    merger.add(newLineString(20, 20, 30, 30));
    merger.add(newLineString(30, 30, 40, 40));
    assertEquals(
      List.of(newLineString(20, 20, 30, 30, 40, 40)),
      merger.getMergedLineStrings(10, -1)
    );

    // removes first short stubs, merges again, and only then 
    // removes non-stubs that are too short
    // stub = linestring that has at least one disconnected end
    merger = new LoopLineMerger();

    /**
     *            0   20  30   50
     *       0    o----o--o----o
     *                 |  |
     *       10        o  o
     */
    merger.add(newLineString(0, 0, 20, 0));
    merger.add(newLineString(20, 0, 30, 0));
    merger.add(newLineString(30, 0, 50, 0));
    merger.add(newLineString(20, 0, 20, 10));
    merger.add(newLineString(30, 0, 30, 10));

    assertEquals(
      List.of(newLineString(0, 0, 20, 0, 30, 0, 50, 0)),
      merger.getMergedLineStrings(15, -1)
    );
  }
  
}
