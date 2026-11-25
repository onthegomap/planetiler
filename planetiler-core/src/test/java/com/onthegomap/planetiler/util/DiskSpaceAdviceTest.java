package com.onthegomap.planetiler.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DiskSpaceAdviceTest {

  private static final IOException DISK_FULL = new IOException("No space left on device");
  @TempDir
  Path tempDir;
  private Path fullDisk;
  private Path bigDisk;

  @BeforeEach
  void setup() {
    fullDisk = tempDir.resolve("full");
    bigDisk = tempDir.resolve("big");
  }

  private DiskSpaceAdvice advice(List<DiskSpaceAdvice.Source> sources, Path tmpDir, Path output,
    boolean compressTemp) {
    return new DiskSpaceAdvice(sources, tmpDir, output, compressTemp,
      path -> path.toAbsolutePath().startsWith(fullDisk) ? new DiskSpaceAdvice.Disk("full", 10) :
        new DiskSpaceAdvice.Disk("big", 1L << 40));
  }

  private DiskSpaceAdvice.Source source(String id, Path path, int bytes, boolean freeAfterReading) throws IOException {
    Files.createDirectories(path.getParent());
    Files.write(path, new byte[bytes]);
    return new DiskSpaceAdvice.Source(id, path, freeAfterReading);
  }

  @Test
  void testOtherFailuresAreNotExplained() {
    var advice = advice(List.of(), bigDisk.resolve("tmp"), bigDisk.resolve("out.pmtiles"), false);
    assertEquals(Optional.empty(), advice.explainFailure(new IllegalStateException("bug"), true));
    // an I/O error only counts as a full disk when the OS says so or a disk is nearly full
    assertEquals(Optional.empty(), advice.explainFailure(new IOException("Input/output error"), true));
  }

  @Test
  void testTemporaryFilesFillUpDisk() {
    var advice = advice(List.of(), fullDisk.resolve("tmp"), bigDisk.resolve("out.pmtiles"), false);
    var failure = new RuntimeException(new ExecutionException(DISK_FULL));
    assertEquals(Optional.of("""
      Ran out of disk space on full. Free up space on that disk, or:
        - write temporary files to another disk with --tmpdir=<dir>
        - use --compress-temp for smaller temporary files"""), advice.explainFailure(failure, false));
  }

  @Test
  void testNoCompressTempSuggestionWhenAlreadyOn() {
    var advice = advice(List.of(), fullDisk.resolve("tmp"), bigDisk.resolve("out.pmtiles"), true);
    assertEquals(Optional.of("""
      Ran out of disk space on full. Free up space on that disk, or:
        - write temporary files to another disk with --tmpdir=<dir>"""), advice.explainFailure(DISK_FULL, false));
  }

  @Test
  void testOtherIoErrorsCountWhenDiskIsNearlyFull() {
    var advice = advice(List.of(), bigDisk.resolve("tmp"), fullDisk.resolve("out.pmtiles"), false);
    String expected = """
      Ran out of disk space on full. Free up space on that disk, or:
        - write the output to another disk with --output=<file>""";
    assertEquals(Optional.of(expected),
      advice.explainFailure(new UncheckedIOException(new IOException("Input/output error")), false));
    // a memory-mapped file on a full disk
    assertEquals(Optional.of(expected),
      advice.explainFailure(new InternalError("a fault occurred in an unsafe memory access operation"), false));
  }

  @Test
  void testSourcesOnFullDisk() throws IOException {
    Path sourcesDir = fullDisk.resolve("sources");
    var sources = List.of(
      source("osm", sourcesDir.resolve("data.osm.pbf"), 3, false),
      // goes under data/sources instead of getting a line of its own
      source("natural_earth", sourcesDir.resolve("ne").resolve("ne.zip"), 7, false),
      // already deleted once read, so not suggested
      source("water", sourcesDir.resolve("water.zip"), 9, true)
    );
    var advice = advice(sources, bigDisk.resolve("tmp"), fullDisk.resolve("out.pmtiles"), false);

    String whileReading = """
      Ran out of disk space on full. Free up space on that disk, or:
        - move the sources in %s to another disk
        - write the output to another disk with --output=<file>""".formatted(FileUtils.displayPath(sourcesDir));
    assertEquals(Optional.of(whileReading), advice.explainFailure(DISK_FULL, false));
    String afterReading = """
      Ran out of disk space on full. Free up space on that disk, or:
        - move the sources in %s to another disk
        - write the output to another disk with --output=<file>
        - use --free-natural-earth-after-read to delete %s once it's read"""
      .formatted(FileUtils.displayPath(sourcesDir), FileUtils.displayPath(sources.get(1).path()));
    assertEquals(Optional.of(afterReading), advice.explainFailure(DISK_FULL, true));
  }

  @Test
  void testUpFrontCheckHints() throws IOException {
    var osm = source("osm", bigDisk.resolve("data.osm.pbf"), 3, false);
    var advice = advice(List.of(osm), bigDisk.resolve("tmp"), bigDisk.resolve("out.pmtiles"), false);
    assertEquals("""
      Free up space on that disk, or:
        - write temporary files to another disk with --tmpdir=<dir>
        - use --compress-temp for smaller temporary files""", advice.readPhaseHint());
    assertEquals("""
      Free up space on that disk, or:
        - write temporary files to another disk with --tmpdir=<dir>
        - write the output to another disk with --output=<file>
        - use --compress-temp for smaller temporary files
        - use --free-osm-after-read to delete %s once it's read""".formatted(FileUtils.displayPath(osm.path())),
      advice.writePhaseHint());
  }
}
