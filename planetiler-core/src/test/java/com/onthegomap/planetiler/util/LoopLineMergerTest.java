package com.onthegomap.planetiler.util;

import static com.onthegomap.planetiler.TestUtils.newLineString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.onthegomap.planetiler.TestUtils;
import com.onthegomap.planetiler.geo.GeoUtils;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.operation.linemerge.LineMerger;

class LoopLineMergerTest {

  //  @Test
  //  void testSplit() {
  //    var merger = new LoopLineMerger();
  //
  //    // splits linestrings into linear segments
  //    assertEquals(
  //      List.of(
  //        newLineString(10, 10, 20, 20),
  //        newLineString(20, 20, 30, 30)
  //      ),
  //      merger.split(newLineString(10, 10, 20, 20, 30, 30))
  //    );
  //
  //    // does not keep zero length linestrings
  //    assertEquals(0, merger.split(newLineString(10, 10, 10, 10)).size());
  //
  //    // rounds coordinates to 1/16 grid
  //    merger = new LoopLineMerger();
  //    assertEquals(
  //      List.of(
  //        newLineString(10.0625, 10, 20, 20),
  //        newLineString(20, 20, 30, 30)
  //      ),
  //      merger.split(newLineString(10.0624390, 10, 20, 20, 30, 30))
  //    );
  //
  //    // rounds coordinates to 0.25 grid
  //    merger = new LoopLineMerger()
  //      .setPrecisionModel(new PrecisionModel(-0.25));
  //
  //    assertEquals(
  //      List.of(
  //        newLineString(10.25, 10, 20, 20),
  //        newLineString(20, 20, 30, 30)
  //      ),
  //      merger.split(newLineString(10.2509803497, 10, 20, 20, 30, 30))
  //    );
  //  }

  //  @Test
  //  void testConcat() {
  //    // concatenates forward A followed by forward B
  //    assertEquals(
  //      newLineString(10, 10, 20, 20, 30, 30),
  //      LoopLineMerger.concat(
  //        newLineString(10, 10, 20, 20),
  //        newLineString(20, 20, 30, 30)
  //      )
  //    );
  //
  //    // concatenates forward B followed by forward A
  //    assertEquals(
  //      newLineString(10, 10, 20, 20, 30, 30),
  //      LoopLineMerger.concat(
  //        newLineString(20, 20, 30, 30),
  //        newLineString(10, 10, 20, 20)
  //      )
  //    );
  //
  //    // concatenates A and B with same start point to backward A forward B
  //    assertEquals(
  //      newLineString(10, 10, 20, 20, 30, 30),
  //      LoopLineMerger.concat(
  //        newLineString(20, 20, 10, 10),
  //        newLineString(20, 20, 30, 30)
  //      )
  //    );
  //
  //    // concatenates A and B with same end point to forward A backward B
  //    assertEquals(
  //      newLineString(10, 10, 20, 20, 30, 30),
  //      LoopLineMerger.concat(
  //        newLineString(10, 10, 20, 20),
  //        newLineString(30, 30, 20, 20)
  //      )
  //    );
  //
  //    // TODO: handle and test the case when A/B do not share end/start points
  //  }

  @Test
  void testMergeTouchingLinestrings() {
    var merger = new LoopLineMerger()
      .setMinLength(-1)
      .setLoopMinLength(-1);

    merger.add(newLineString(10, 10, 20, 20));
    merger.add(newLineString(20, 20, 30, 30));
    assertEquals(
      List.of(newLineString(10, 10, 20, 20, 30, 30)),
      merger.getMergedLineStrings()
    );
  }

  @Test
  void testKeepTwoSeparateLinestring() {
    var merger = new LoopLineMerger()
      .setMinLength(-1)
      .setLoopMinLength(-1);

    merger.add(newLineString(10, 10, 20, 20));
    merger.add(newLineString(30, 30, 40, 40));
    assertEquals(
      List.of(
        newLineString(10, 10, 20, 20),
        newLineString(30, 30, 40, 40)
      ),
      merger.getMergedLineStrings()
    );
  }

  @Test
  void testDoesNotOvercountAlreadyAddedLines() {
    var merger = new LoopLineMerger()
      .setMinLength(-1)
      .setLoopMinLength(-1);

    merger.add(newLineString(10, 10, 20, 20));
    merger.add(newLineString(20, 20, 30, 30));
    merger.add(newLineString(20, 20, 30, 30));
    assertEquals(
      List.of(newLineString(10, 10, 20, 20, 30, 30)),
      merger.getMergedLineStrings()
    );
  }

  @Test
  void testSplitLinestringsBeforeMerging() {
    var merger = new LoopLineMerger()
      .setMinLength(-1)
      .setLoopMinLength(-1);

    merger.add(newLineString(10, 10, 20, 20, 30, 30));
    merger.add(newLineString(20, 20, 30, 30, 40, 40));
    assertEquals(
      List.of(newLineString(40, 40, 30, 30, 20, 20, 10, 10)),
      merger.getMergedLineStrings()
    );
  }

  @Test
  void testRoundCoordinatesBeforeMerging() {
    var merger = new LoopLineMerger()
      .setMinLength(-1)
      .setLoopMinLength(-1);

    merger.add(newLineString(10.00043983098, 10, 20, 20));
    merger.add(newLineString(20, 20, 30, 30));
    assertEquals(
      List.of(newLineString(10, 10, 20, 20, 30, 30)),
      merger.getMergedLineStrings()
    );
  }

  @Test
  void testRemoveSmallLoops() {
    var merger = new LoopLineMerger()
      .setMinLength(-1)
      .setLoopMinLength(100);

    /*
     * 10 20 30 40 10 o--o--o \ | \| 20 o--o
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
          40, 20,
          30, 20,
          20, 10,
          10, 10
        )
      ),
      merger.getMergedLineStrings()
    );
  }

  @Test
  void testDoNotRemoveLargeLoops() {
    var merger = new LoopLineMerger()
      .setMinLength(-1)
      .setLoopMinLength(0.001);

    /*
     * 10 20 30 40 10 o--o--o \ | \| 20 o--o
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
          20, 10
        ),
        newLineString(
          20, 10,
          30, 10,
          30, 20
        ),
        newLineString(
          20, 10,
          30, 20
        ),
        newLineString(
          30, 20,
          40, 20
        )
      ),
      merger.getMergedLineStrings()
    );
  }

  @Test
  void testRemoveShortStubs() {
    var merger = new LoopLineMerger()
      .setMinLength(10)
      .setLoopMinLength(-1);

    merger.add(newLineString(10, 10, 11, 11));
    merger.add(newLineString(20, 20, 30, 30));
    merger.add(newLineString(30, 30, 40, 40));
    assertEquals(
      List.of(newLineString(20, 20, 30, 30, 40, 40)),
      merger.getMergedLineStrings()
    );
  }

  @Test
  void testRemovesShortStubsTheNonStubsThatAreTooShort() {
    var merger = new LoopLineMerger()
      .setMinLength(15)
      .setLoopMinLength(-1);

    /*
     * 0 20 30 50 0 o----o--o----o | | 10 o o
     */
    merger.add(newLineString(0, 0, 20, 0));
    merger.add(newLineString(20, 0, 30, 0));
    merger.add(newLineString(30, 0, 50, 0));
    merger.add(newLineString(20, 0, 20, 10));
    merger.add(newLineString(30, 0, 30, 10));

    assertEquals(
      List.of(newLineString(50, 0, 30, 0, 20, 0, 0, 0)),
      merger.getMergedLineStrings()
    );
  }

  //  @Test
  //  void testHasPointAppearingMoreThanTwice() {
  //
  //    // can revisit starting node once
  //    assertEquals(
  //      false,
  //      LoopLineMerger.hasPointAppearingMoreThanTwice(
  //        List.of(
  //          newLineString(10, 0, 20, 0),
  //          newLineString(20, 0, 20, 10),
  //          newLineString(20, 10, 10, 0)
  //        )
  //      )
  //    );
  //
  //    // cannot revisit starting node and continue on
  //    assertEquals(
  //      true,
  //      LoopLineMerger.hasPointAppearingMoreThanTwice(
  //        List.of(
  //          newLineString(10, 0, 20, 0),
  //          newLineString(20, 0, 20, 10),
  //          newLineString(20, 10, 10, 0),
  //          newLineString(10, 0, 10, 10)
  //        )
  //      )
  //    );
  //  }
  //
  //  @Test
  //  void testFindAllPaths() {
  //    // finds all paths and orders them by length
  //    var merger = new LoopLineMerger();
  //    /**
  //     * 10 20 30 10 o-----o |\ | | \ | 20 o--o--o
  //     */
  //    merger.add(newLineString(10, 10, 30, 10));
  //    merger.add(newLineString(10, 10, 10, 20));
  //    merger.add(newLineString(10, 10, 20, 20));
  //    merger.add(newLineString(10, 20, 20, 20));
  //    merger.add(newLineString(20, 20, 30, 20));
  //    merger.add(newLineString(30, 20, 30, 10));
  //
  //    var allPaths = merger.findAllPaths(newPoint(10, 10), newPoint(20, 20), 1000);
  //
  //    assertEquals(3, allPaths.size());
  //    assertEquals(
  //      List.of(newLineString(10, 10, 20, 20)),
  //      allPaths.get(0)
  //    );
  //    assertEquals(
  //      List.of(
  //        newLineString(10, 10, 10, 20),
  //        newLineString(10, 20, 20, 20)
  //      ),
  //      allPaths.get(1)
  //    );
  //    assertEquals(
  //      List.of(
  //        newLineString(10, 10, 30, 10),
  //        newLineString(30, 10, 30, 20),
  //        newLineString(30, 20, 20, 20)
  //      ),
  //      allPaths.get(2)
  //    );
  //  }

  //  @Disabled
  @ParameterizedTest
  @CsvSource({
    "mergelines_1759_point_line.wkb.gz,0,4",
    "mergelines_1759_point_line.wkb.gz,1,2",

    "mergelines_200433_lines.wkb.gz,0,35271",
    "mergelines_200433_lines.wkb.gz,0.1,24051",
    "mergelines_200433_lines.wkb.gz,1,1607",

    "mergelines_239823_lines.wkb.gz,0,19669",
    "mergelines_239823_lines.wkb.gz,0.1,14196",
    "mergelines_239823_lines.wkb.gz,1,1673",
  })
  void testOnRealWorldData(String file, double minLengths, int expected)
    throws IOException, ParseException {
    Geometry geom = new WKBReader(GeoUtils.JTS_FACTORY).read(
      Gzip.gunzip(Files.readAllBytes(TestUtils.pathToResource("mergelines").resolve(file))));
    var merger = new LoopLineMerger();
    merger.setMinLength(minLengths);
    merger.setLoopMinLength(minLengths);
    merger.add(geom);
    var merged = merger.getMergedLineStrings();
    Set<List<Coordinate>> lines = new HashSet<>();
    var merger2 = new LineMerger();
    for (var line : merged) {
      merger2.add(line);
      assertTrue(lines.add(Arrays.asList(line.getCoordinates())), "contained duplicate: " + line);
      if (minLengths > 0) {
        assertTrue(line.getLength() >= minLengths, "line < " + minLengths + ": " + line);
      }
    }
    // ensure there are no more opportunities for simplification found by JTS:
    assertEquals(merged.size(), merger2.getMergedLineStrings().size());
    assertEquals(expected, merged.size());
  }

}
