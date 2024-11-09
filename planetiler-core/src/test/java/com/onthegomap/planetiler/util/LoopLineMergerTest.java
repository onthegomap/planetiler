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

  @Test
  void testMergeCarriagewaysWithOneSplitShorterThanLoopMinLength() {
    var merger = new LoopLineMerger()
      .setMinLength(20)
      .setLoopMinLength(20);

    merger.add(newLineString(0, 0, 10, 0, 20, 0, 30, 0));
    merger.add(newLineString(30, 0, 20, 0, 15, 1, 10, 0, 0, 0));

    assertEquals(
      List.of(newLineString(30, 0, 20, 0, 10, 0, 0, 0)),
      merger.getMergedLineStrings()
    );
  }

  @Test
  void testMergeCarriagewaysWithOneSplitLongerThanLoopMinLength() {
    var merger = new LoopLineMerger()
      .setMinLength(5)
      .setLoopMinLength(5);

    merger.add(newLineString(0, 0, 10, 0, 20, 0, 30, 0));
    merger.add(newLineString(30, 0, 20, 0, 15, 1, 10, 0, 0, 0));

    assertEquals(
      // ideally loop merging should connect long line strings and represent loops as separate segments off of the edges
      List.of(newLineString(30, 0, 20, 0, 10, 0, 0, 0), newLineString(20, 0, 15, 1, 10, 0)),
      merger.getMergedLineStrings()
    );
  }

  @Test
  void testMergeCarriagewaysWithTwoSplits() {
    var merger = new LoopLineMerger()
      .setMinLength(20)
      .setLoopMinLength(20);

    merger.add(newLineString(0, 0, 10, 0, 20, 0, 30, 0, 40, 0));
    merger.add(newLineString(40, 0, 30, 0, 25, 5, 20, 0, 15, 5, 10, 0, 0, 0));

    assertEquals(
      List.of(newLineString(40, 0, 30, 0, 20, 0, 10, 0, 0, 0)),
      merger.getMergedLineStrings()
    );
  }

  @Test
  void testRealWorldHarkingen() {
    var merger = new LoopLineMerger()
      .setMinLength(4 * 0.0625)
      .setLoopMinLength(8 * 0.0625);

    merger.add(newLineString(99.185791015625,109.83056640625, 99.202392578125,109.8193359375, 99.21337890625,109.810302734375, 99.222412109375,109.8017578125, 99.229736328125,109.793701171875, 99.241943359375,109.779541015625));
    merger.add(newLineString(98.9931640625,109.863525390625, 99.005126953125,109.862060546875, 99.01708984375,109.86083984375, 99.028564453125,109.85986328125, 99.040283203125,109.859375, 99.0712890625,109.85791015625, 99.08203125,109.857421875, 99.093017578125,109.856689453125, 99.104248046875,109.855712890625, 99.115478515625,109.8544921875, 99.12646484375,109.852783203125, 99.1376953125,109.850341796875, 99.1474609375,109.84765625, 99.15673828125,109.844482421875, 99.166748046875,109.84033203125, 99.175537109375,109.836181640625, 99.185791015625,109.83056640625));
    merger.add(newLineString(99.162841796875,109.812744140625, 99.0966796875,109.824462890625, 99.055419921875,109.832275390625, 99.008544921875,109.842041015625, 98.967529296875,109.8525390625, 98.8818359375,109.875244140625));
    merger.add(newLineString(98.879150390625,109.885498046875, 98.94091796875,109.86572265625, 98.968017578125,109.859130859375, 99.017578125,109.847412109375, 99.056396484375,109.83984375, 99.09814453125,109.831298828125, 99.163330078125,109.81982421875));
      
    assertEquals(
      1,
      merger.getMergedLineStrings().size()
    );
  }

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

    "i90.wkb.gz,0,95",
    "i90.wkb.gz,1,65",
    "i90.wkb.gz,20,4",
    "i90.wkb.gz,30,1",
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
