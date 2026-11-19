package com.onthegomap.planetiler.stats;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.onthegomap.planetiler.TestUtils;
import com.onthegomap.planetiler.util.AnsiColors;
import com.onthegomap.planetiler.util.Format;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class SourceProgressLoggerTest {
  private static final String NL = System.lineSeparator();
  private final Format format = Format.defaultInstance();
  // counts get their values before they are set on a source, so every rate shows 0/s
  private final AtomicLong downloaded = new AtomicLong(50);
  private final AtomicBoolean downloadFinished = new AtomicBoolean();
  private final AtomicLong read = new AtomicLong(7);

  // the logger ends lines with the system line separator, text blocks with \n
  private static String lines(String textBlock) {
    return textBlock.replace("\n", NL);
  }

  private SourceProgressLogger logger(SourceProgress... sources) {
    return new SourceProgressLogger(format, List.of(sources));
  }

  private SourceProgress downloading(String name) {
    return new SourceProgress(name).setDownload(downloaded::get, () -> 100, downloadFinished::get);
  }

  private SourceProgress reading(String name) {
    var source = new SourceProgress(name).setRead(read::get, 18).setTrackWritten();
    source.start();
    return source;
  }

  @Test
  void testShowsNothingUntilASourceDownloadsOrReads() {
    assertEquals("", logger(new SourceProgress("a")).table());
  }

  @Test
  void testLinesUpRowsWhenOnlySomeDownload() {
    var logger = logger(downloading("a"), reading("long_name"));

    assertEquals(lines("""
      a         download: [   50  50%    0/s ]
      long_name download:                      read: [    7  39%    0/s ] write: [    0    0/s ]"""),
      TestUtils.withoutColors(logger.table()));
  }

  @Test
  void testShowsFinishedRowOnceMore() {
    var source = reading("a");
    var logger = logger(source);
    logger.table();
    source.finish();

    assertEquals("a read: [    7  39%    0/s ] write: [    0    0/s ]", logger.table());
    assertEquals("", logger.table());
  }

  @Test
  void testKeepsRowGreenWhileFeaturesAreLeftToProcess() {
    var source = reading("a");
    var workLeft = source.workLeftCounterForThread();
    workLeft.inc();
    source.finish();
    var logger = logger(source);

    assertEquals("a read: " + AnsiColors.green("[    7  39%    0/s ]") + " write: [    0    0/s ]", logger.table());
    workLeft.incBy(-1);
    assertEquals("a read: [    7  39%    0/s ] write: [    0    0/s ]", logger.table());
    assertEquals("", logger.table());
  }

  @Test
  void testShowsDownloadThatFinishedBeforeTheFirstLog() {
    var logger = logger(downloading("a"));
    downloaded.set(100);
    downloadFinished.set(true);

    String table = TestUtils.withoutColors(logger.table());
    assertTrue(table.startsWith("a download: [  100 100% "), table);
    assertEquals("", logger.table());
  }

  @Test
  void testShowsCellsOfOneSource() {
    var source = new SourceProgress("a");
    var logger = logger(source);

    assertEquals(" read:", logger.cells());
    source.setRead(read::get, 0).setTrackWritten();
    assertEquals(" read: [    7    -    0/s ] write: [    0    0/s ]", logger.cells());
  }

  @Test
  void testShowsSourceReadBetweenTwoLogs() {
    var source = new SourceProgress("a");
    var logger = logger(source);
    source.setRead(read::get, 18).setTrackWritten();
    source.start();
    source.finish();

    assertEquals("a read: [    7  39%    0/s ] write: [    0    0/s ]", logger.table());
    assertEquals("", logger.table());
  }
}
