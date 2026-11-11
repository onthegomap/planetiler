package com.onthegomap.planetiler.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.util.Downloader.ByteRange;
import com.onthegomap.planetiler.util.RequestPlanner.Plan;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.LongStream;
import org.junit.jupiter.api.Test;

class RequestPlannerTest {

  private static final long MB = 1_000_000;
  private static final long ROW_GROUP = 2_400_000;
  private static final PlanetilerConfig CONFIG = PlanetilerConfig.defaults();
  private static final RequestPlanner DEFAULTS =
    new RequestPlanner(CONFIG.downloadThreads(), CONFIG.downloadChunkSizeMB() * MB);

  private static ByteRange range(long offset, long length) {
    return new ByteRange(offset, length);
  }

  private static List<ByteRange> rowGroups(long offset, long size, int count) {
    return LongStream.range(0, count).mapToObj(i -> range(offset + i * size, size)).toList();
  }

  private static List<ByteRange> concat(List<ByteRange> a, List<ByteRange> b) {
    List<ByteRange> result = new ArrayList<>(a);
    result.addAll(b);
    return result;
  }

  private static List<Long> lengths(Plan plan) {
    return plan.httpRequests().stream().map(ByteRange::length).toList();
  }

  private static Plan plan(RequestPlanner planner, List<ByteRange> needed) {
    return plan(planner, needed, needed.getLast().end());
  }

  private static Plan plan(RequestPlanner planner, List<ByteRange> needed, long fileLength) {
    var plan = planner.plan(needed, fileLength);
    var nonEmpty = needed.stream().filter(range -> range.length() > 0).toList();
    long end = Long.MIN_VALUE;
    for (var request : plan.httpRequests()) {
      assertTrue(request.offset() >= end, "requests out of order or overlapping: " + plan);
      assertTrue(request.length() > 0 && request.length() <= planner.maxRequestBytes(), "bad size: " + request);
      assertTrue(isNeeded(nonEmpty, request.offset()), "starts in a gap: " + request);
      assertTrue(isNeeded(nonEmpty, request.end() - 1), "ends in a gap: " + request);
      end = request.end();
    }
    for (var range : nonEmpty) {
      long covered = plan.httpRequests().stream()
        .mapToLong(r -> Math.max(0, Math.min(r.end(), range.end()) - Math.max(r.offset(), range.offset())))
        .sum();
      assertEquals(range.length(), covered, "not fetched: " + range);
    }
    assertTrue(plan.maxInFlight() >= 1);
    return plan;
  }

  private static boolean isNeeded(List<ByteRange> needed, long position) {
    return needed.stream().anyMatch(range -> position >= range.offset() && position < range.end());
  }

  @Test
  void testMonacoOsmPbf() {
    var plan = plan(DEFAULTS, List.of(range(0, 600_000)));
    assertEquals(List.of(range(0, 600_000)), plan.httpRequests());
  }

  @Test
  void testMassachusettsOsmPbf() {
    var plan = plan(DEFAULTS, List.of(range(0, 310 * MB)));
    assertEquals(List.of(77_500_000L, 77_500_000L, 77_500_000L, 77_500_000L), lengths(plan));
    assertEquals(4, plan.maxInFlight());
  }

  @Test
  void testSwitzerlandOsmPbf() {
    var plan = plan(DEFAULTS, List.of(range(0, 480 * MB)));
    assertEquals(List.of(96 * MB, 96 * MB, 96 * MB, 96 * MB, 96 * MB), lengths(plan));
    assertEquals(4, plan.maxInFlight());
  }

  @Test
  void testPlanetOsmPbf() {
    var whole = List.of(range(0, 95_600 * MB));
    var plan = plan(DEFAULTS, whole);
    assertEquals(956, plan.httpRequests().size());
    assertTrue(plan.httpRequests().stream().allMatch(request -> request.length() == 100 * MB));
    assertEquals(4, plan.maxInFlight());

    // raising download_threads is how big machines get more requests at once, with the same requests
    var faster = plan(new RequestPlanner(64, 100 * MB), whole);
    assertEquals(plan.httpRequests(), faster.httpRequests());
    assertEquals(64, faster.maxInFlight());
  }

  @Test
  void testWholeWaterParquet() {
    var plan = plan(DEFAULTS, List.of(range(0, 1_000 * MB)));
    assertEquals(10, plan.httpRequests().size());
    assertEquals(4, plan.maxInFlight());
  }

  @Test
  void testWaterParquetInSmallBounds() {
    // 5 clusters of 4 row groups of 512 KB, far apart: one request per cluster, many at once since each is small
    List<ByteRange> needed = new ArrayList<>();
    for (int cluster = 0; cluster < 5; cluster++) {
      needed.addAll(rowGroups(cluster * 100 * MB, 512_000, 4));
    }
    var plan = plan(DEFAULTS, needed);
    assertEquals(List.of(2_048_000L, 2_048_000L, 2_048_000L, 2_048_000L, 2_048_000L), lengths(plan));
    assertEquals(16, plan.maxInFlight());
  }

  @Test
  void testFewRowGroupsInSmallBounds() {
    // like Monaco buildings: 2 neighbouring row groups of a large file get split into a request each to fetch them at
    // once
    var plan = plan(DEFAULTS, rowGroups(50 * MB, ROW_GROUP, 2), 225 * ROW_GROUP);
    assertEquals(rowGroups(50 * MB, ROW_GROUP, 2), plan.httpRequests());
    assertEquals(16, plan.maxInFlight());
  }

  @Test
  void testRowGroupsInStateBounds() {
    // like Massachusetts buildings in one file: a few clusters of neighbouring row groups, a fifth of the file, split
    // into requests of about 1/16 of the 108 MB needed to run 16 at once
    var needed = concat(concat(rowGroups(0, ROW_GROUP, 10), rowGroups(200 * MB, ROW_GROUP, 20)),
      rowGroups(400 * MB, ROW_GROUP, 15));
    var plan = plan(DEFAULTS, needed, 225 * ROW_GROUP);
    assertEquals(23, plan.httpRequests().size());
    assertTrue(plan.httpRequests().stream().allMatch(request -> request.length() <= 2 * ROW_GROUP), plan.toString());
    assertEquals(16, plan.maxInFlight());
  }

  @Test
  void testWholeFileOfRowGroupsIsLikeFullDownload() {
    // planet bounds need every row group of every file, which gets fetched like a full download of the file
    long fileBytes = 225 * ROW_GROUP;
    var cached = plan(DEFAULTS, rowGroups(0, ROW_GROUP, 225));
    var full = plan(DEFAULTS, List.of(range(0, fileBytes)));
    assertEquals(6, cached.httpRequests().size());
    assertEquals(full.httpRequests().size(), cached.httpRequests().size());
    assertEquals(full.maxInFlight(), cached.maxInFlight());
    // split between row groups, not inside them like a full download
    assertTrue(cached.httpRequests().stream().allMatch(request -> request.length() % ROW_GROUP == 0),
      cached.toString());
  }

  @Test
  void testHalfAFileOfRowGroups() {
    var plan = plan(DEFAULTS, rowGroups(0, ROW_GROUP, 112), 225 * ROW_GROUP);
    assertEquals(Collections.nCopies(8, 14 * ROW_GROUP), lengths(plan));
    assertEquals(8, plan.maxInFlight());
  }

  @Test
  void testMostOfAFileOfRowGroups() {
    // skipping a few row groups leaves requests as large as a full download's, so no more of them at once
    var needed = rowGroups(0, ROW_GROUP, 225).stream().filter(range -> range.offset() % (10 * ROW_GROUP) != 0).toList();
    var plan = plan(DEFAULTS, needed);
    assertEquals(6, plan.httpRequests().size());
    assertEquals(4, plan.maxInFlight());
  }

  @Test
  void testMergesGapsUpToOnePercentOfBytesNeeded() {
    // 2 MB needed: gaps up to 20 KB get fetched instead of making another request
    var merged = plan(DEFAULTS, List.of(range(0, MB), range(MB + 20_000, MB)));
    assertEquals(List.of(range(0, 2 * MB + 20_000)), merged.httpRequests());
    assertEquals(2 * MB + 20_000, merged.bytes());

    var separate = plan(DEFAULTS, List.of(range(0, MB), range(MB + 20_001, MB)));
    assertEquals(List.of(range(0, MB), range(MB + 20_001, MB)), separate.httpRequests());
  }

  @Test
  void testSplitsBetweenRangesInsteadOfFetchingGaps() {
    // the gap merges the ranges into one span, but splitting it into 4 requests cuts at the gap instead of fetching it
    var plan = plan(DEFAULTS, List.of(range(0, 50 * MB), range(50 * MB + 500_000, 50 * MB)));
    assertEquals(100 * MB, plan.bytes());
  }

  @Test
  void testSplitsRangesLargerThanARequest() {
    var needed = concat(concat(rowGroups(0, ROW_GROUP, 3), List.of(range(3 * ROW_GROUP, 250 * MB))),
      rowGroups(3 * ROW_GROUP + 250 * MB, ROW_GROUP, 3));
    var plan = plan(DEFAULTS, needed);
    assertTrue(plan.httpRequests().size() >= 3, plan.toString());
  }

  @Test
  void testSmallMaxRequestBytes() {
    var plan = plan(new RequestPlanner(4, MB), List.of(range(100, 2_500_000)));
    assertEquals(List.of(range(100, 833_333), range(833_433, 833_333), range(1_666_766, 833_334)), plan.httpRequests());
  }

  @Test
  void testThreadsScaleSparseReads() {
    List<ByteRange> scattered = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      scattered.add(range(i * 100 * MB, 512 << 10));
    }
    assertEquals(16, plan(DEFAULTS, scattered).maxInFlight());
    assertEquals(4, plan(new RequestPlanner(1, 100 * MB), scattered).maxInFlight());
    assertEquals(64, plan(new RequestPlanner(16, 100 * MB), scattered).maxInFlight());
    assertEquals(256, plan(new RequestPlanner(64, 100 * MB), scattered).maxInFlight());
    assertEquals(100, plan(DEFAULTS, scattered).httpRequests().size());
  }

  @Test
  void testNothingNeeded() {
    assertEquals(Plan.EMPTY, DEFAULTS.plan(List.of(), 100));
    assertEquals(Plan.EMPTY, DEFAULTS.plan(List.of(range(10, 0), range(20, 0)), 100));
    assertEquals(0, Plan.EMPTY.bytes());
    assertEquals(0, Plan.EMPTY.bytesInFlight());
  }

  @Test
  void testSkipsEmptyRanges() {
    var plan = plan(DEFAULTS, List.of(range(0, 0), range(10, 100), range(500, 0)));
    assertEquals(List.of(range(10, 100)), plan.httpRequests());
  }

  @Test
  void testBytesInFlight() {
    var plan = DEFAULTS.plan(List.of(range(0, 310 * MB)), 310 * MB);
    assertEquals(310 * MB, plan.bytes());
    assertEquals(310 * MB, plan.bytesInFlight());
    var planet = DEFAULTS.plan(List.of(range(0, 95_600 * MB)), 95_600 * MB);
    assertEquals(400 * MB, planet.bytesInFlight());
  }

  @Test
  void testRejectsRangesOutOfOrderOverlappingOrPastTheEnd() {
    var outOfOrder = List.of(range(100, 10), range(0, 10));
    assertThrows(IllegalArgumentException.class, () -> DEFAULTS.plan(outOfOrder, 1_000));
    var overlapping = List.of(range(0, 10), range(5, 10));
    assertThrows(IllegalArgumentException.class, () -> DEFAULTS.plan(overlapping, 1_000));
    var pastTheEnd = List.of(range(0, 10), range(95, 10));
    assertThrows(IllegalArgumentException.class, () -> DEFAULTS.plan(pastTheEnd, 100));
  }

  @Test
  void testRejectsBadLimits() {
    assertThrows(IllegalArgumentException.class, () -> new RequestPlanner(0, MB));
    assertThrows(IllegalArgumentException.class, () -> new RequestPlanner(4, 0));
  }
}
