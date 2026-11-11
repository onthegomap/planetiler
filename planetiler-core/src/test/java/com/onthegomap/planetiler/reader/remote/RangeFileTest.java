package com.onthegomap.planetiler.reader.remote;

import static org.junit.jupiter.api.Assertions.*;

import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.util.Downloader;
import com.onthegomap.planetiler.util.FileUtils;
import com.onthegomap.planetiler.util.RequestPlanner;
import com.onthegomap.planetiler.util.StubDownloader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

abstract class RangeFileTest {

  static final String URL = "http://host/file.parquet";
  // larger than the tail fetched when opening a remote file, so reads at the start need range requests
  static final byte[] CONTENTS = contents(3_000_000);
  // like parquet files, which fetch their footer when they get opened
  static final int TAIL_BYTES = 1 << 20;
  static final PlanetilerConfig CONFIG = PlanetilerConfig.from(Arguments.of("download_chunk_size_mb", "1"));
  @TempDir
  Path tmp;
  StubDownloader downloader = new StubDownloader(CONFIG).put(URL, CONTENTS);
  RangeFile file;

  static byte[] contents(int length) {
    byte[] result = new byte[length];
    for (int i = 0; i < length; i++) {
      result[i] = (byte) (i * 31 + (i >> 8));
    }
    return result;
  }

  abstract RangeFile open() throws IOException;

  @BeforeEach
  void setup() throws IOException {
    file = open();
  }

  @AfterEach
  void close() throws IOException {
    file.close();
  }

  void assertReads(RangeFile file, long offset, int length) throws IOException {
    byte[] expected = Arrays.copyOfRange(CONTENTS, (int) offset, (int) offset + length);
    assertArrayEquals(expected, file.read(offset, length), "read " + offset + "+" + length);
    assertArrayEquals(expected, file.readLater(List.of(range(offset, length))).getFirst().get(),
      "readLater " + offset + "+" + length);
  }

  static Downloader.ByteRange range(long offset, long length) {
    return new Downloader.ByteRange(offset, length);
  }

  static byte[] slice(long offset, int length) {
    return Arrays.copyOfRange(CONTENTS, (int) offset, (int) offset + length);
  }

  List<String> ranges() {
    return downloader.requests().stream().map(StubDownloader.Request::range).toList();
  }

  @Test
  void testLength() throws IOException {
    assertEquals(CONTENTS.length, file.length());
  }

  @Test
  void testReads() throws IOException {
    assertReads(file, 0, 10);
    assertReads(file, 0, 0);
    assertReads(file, 999_990, 20);
    assertReads(file, 500_000, 1_500_000);
    assertReads(file, CONTENTS.length - 10L, 10);
    assertReads(file, 0, CONTENTS.length);
  }

  @Test
  void testConcurrentReads() {
    IntStream.range(0, 50).parallel().forEach(i -> {
      try {
        assertReads(file, i * 58_001L, 70_000);
      } catch (IOException e) {
        throw new AssertionError(e);
      }
    });
  }

  static class LocalTest extends RangeFileTest {

    @Override
    RangeFile open() throws IOException {
      Path path = tmp.resolve("file.parquet");
      Files.write(path, CONTENTS);
      return RangeFile.openLocal(path);
    }

    @Test
    void testLocalFileWinsInEveryMode() throws IOException {
      Path path = tmp.resolve("file.parquet");
      for (var mode : DownloadMode.values()) {
        try (var opened = RangeFile.open(path, URL, TAIL_BYTES, mode, false, downloader)) {
          assertInstanceOf(LocalRangeFile.class, opened);
        }
      }
      assertEquals(List.of(), downloader.requests());
    }
  }

  static class HttpTest extends RangeFileTest {

    @Override
    RangeFile open() throws IOException {
      return RangeFile.open(tmp.resolve("file.parquet"), URL, TAIL_BYTES, DownloadMode.STREAMING, false, downloader);
    }

    @Test
    void testOpenFetchesTail() {
      assertEquals(List.of(new StubDownloader.Request(URL, "bytes=-" + TAIL_BYTES, null, null)),
        downloader.requests());
      assertFalse(Files.exists(tmp.resolve("file.parquet")));
    }

    @Test
    void testReadsFromTailMakeNoRequests() throws IOException {
      downloader.clearRequests();
      assertReads(file, CONTENTS.length - TAIL_BYTES, TAIL_BYTES);
      assertEquals(List.of(), downloader.requests());
    }

    @Test
    void testSplitsLargeReads() throws IOException {
      downloader.clearRequests();
      file.read(100, 2_500_000);
      // split evenly into requests no larger than download_chunk_size_mb
      assertEquals(List.of("bytes=100-833432", "bytes=1666766-2500099", "bytes=833433-1666765"),
        downloader.requests().stream().map(StubDownloader.Request::range).sorted().toList());
    }

    @Test
    void testFailsWhenFileChanges() {
      downloader.put(URL, CONTENTS, "\"new\"", null);
      var exception = assertThrows(Downloader.RemoteFileChangedException.class, () -> file.read(0, 10));
      assertTrue(exception.getMessage().contains("--download-fail-if-changed=false"), exception.getMessage());
    }

    @Test
    void testIgnoresValidatorsWithCheckForChangesOff() throws IOException {
      var config = PlanetilerConfig.from(Arguments.of("download_fail_if_changed", "false"));
      var lenient = new StubDownloader(config).put(URL, CONTENTS);
      try (var opened = HttpRangeFile.open(lenient, URL)) {
        lenient.put(URL, CONTENTS, "\"new\"", "new date");
        assertArrayEquals(Arrays.copyOf(CONTENTS, 10), opened.read(0, 10));
        assertEquals(new StubDownloader.Request(URL, "bytes=0-9", null, null), lenient.requests().getLast());

        // a different length can't be the same file
        lenient.put(URL, contents(CONTENTS.length + 1));
        assertThrows(Downloader.RemoteFileChangedException.class, () -> opened.read(0, 10));
      }
    }

    @Test
    void testCountsFetchedBytesWithoutRetries() throws IOException {
      assertEquals(TAIL_BYTES, file.bytesFetched());
      assertEquals(1, file.requestsMade());

      file.read(100, 2_500_000);
      assertEquals(TAIL_BYTES + 2_500_000, file.bytesFetched());
      assertEquals(4, file.requestsMade());

      downloader.failNext(1, 503);
      file.read(0, 10);
      assertEquals(TAIL_BYTES + 2_500_010, file.bytesFetched());
      assertEquals(5, file.requestsMade());
    }

    @Test
    void testToFetchMatchesReads() throws IOException {
      long tailStart = CONTENTS.length - TAIL_BYTES;
      assertFalse(file.needsFetch(tailStart, 100));
      assertEquals(RequestPlanner.Plan.EMPTY, file.planFetch(List.of(range(tailStart, 100))));
      assertTrue(file.needsFetch(tailStart - 5, 10), "reads starting before the tail fetch all of it");
      var plan = file.planFetch(List.of(range(100, 2_500_000)));
      assertEquals(2_500_000, plan.bytes());
      assertEquals(3, plan.httpRequests().size());

      file.read(100, 2_500_000);
      assertEquals(TAIL_BYTES + 2_500_000, file.bytesFetched());
      assertEquals(1 + 3, file.requestsMade());
    }

    @Test
    void testOpenWithoutTailFetchesFirstByte() throws IOException {
      downloader.clearRequests();
      try (var opened = HttpRangeFile.open(downloader, URL)) {
        assertEquals(List.of(new StubDownloader.Request(URL, "bytes=0-0", null, null)), downloader.requests());
        assertEquals(CONTENTS.length, opened.length());
        assertFalse(opened.needsFetch(0, 1));
        assertTrue(opened.needsFetch(0, 10));
        assertReads(opened, 0, 1);
        assertEquals(1, downloader.requests().size());
      }
    }

    @Test
    void testMergesNeighbouringRangesIntoOneRequest() throws IOException {
      downloader.clearRequests();
      var reads = file.readLater(List.of(range(100, 100), range(200, 100), range(300, 100)));
      for (int i = 0; i < reads.size(); i++) {
        assertArrayEquals(slice(100 + i * 100L, 100), reads.get(i).get());
      }
      assertEquals(List.of("bytes=100-399"), ranges());
    }

    @Test
    void testSendsRequestsOnceTheirFirstRangeStarts() throws IOException {
      downloader.clearRequests();
      var reads = file.readLater(List.of(range(0, 10), range(1_000_000, 10)));
      assertEquals(List.of(), ranges(), "nothing gets sent before a range starts");
      assertArrayEquals(slice(0, 10), reads.get(0).get());
      assertEquals(List.of("bytes=0-9"), ranges());
      assertArrayEquals(slice(1_000_000, 10), reads.get(1).get());
      assertEquals(List.of("bytes=0-9", "bytes=1000000-1000009"), ranges());
    }

    @Test
    void testReturnsRangesBeforeLaterOnesInTheSameRequestStart() throws IOException {
      var config = PlanetilerConfig.from(Arguments.of("download_chunk_size_mb", "10"));
      var larger = new StubDownloader(config).put(URL, CONTENTS);
      try (var opened = HttpRangeFile.open(larger, URL)) {
        larger.clearRequests();
        long fetchedBefore = opened.bytesFetched();
        var reads = opened.readLater(List.of(range(100, 600_000), range(600_100, 600_000), range(1_200_100, 600_000)));
        assertArrayEquals(slice(100, 600_000), reads.get(0).get());
        assertEquals(List.of("bytes=100-1800099"),
          larger.requests().stream().map(StubDownloader.Request::range).toList());
        // the request reads 1 MB at a time, and stops in its first MB where the second range starts
        assertEquals(fetchedBefore + (1 << 20), opened.bytesFetched());
        assertArrayEquals(slice(600_100, 600_000), reads.get(1).get());
        assertArrayEquals(slice(1_200_100, 600_000), reads.get(2).get());
      }
    }

    @Test
    void testResumesMidRequestAfterDroppedConnection() throws IOException {
      downloader.clearRequests();
      downloader.truncateNext(1, 150);
      var reads = file.readLater(List.of(range(1000, 100), range(1100, 100), range(1200, 100)));
      for (int i = 0; i < reads.size(); i++) {
        assertArrayEquals(slice(1000 + i * 100L, 100), reads.get(i).get());
      }
      assertEquals(List.of("bytes=1000-1299", "bytes=1150-1299"), ranges());
    }

    @Test
    void testCloseFailsReadsNotStarted() throws IOException {
      var read = file.readLater(List.of(range(0, 10))).getFirst();
      file.close();
      assertThrows(CancellationException.class, read::get);
    }

    @Test
    void testSkipsRemainingRequestsAfterOneFails() throws IOException {
      var config = PlanetilerConfig.from(Arguments.of(Map.of(
        "download_threads", "1",
        "http_retries", "0",
        "download_chunk_size_mb", "10"
      )));
      var failing = new StubDownloader(config).put(URL, contents(40_000_000));
      try (var opened = HttpRangeFile.open(failing, URL)) {
        failing.clearRequests();
        failing.failNext(1, 503);
        assertThrows(IOException.class, () -> opened.read(0, 40_000_000));
        // 4 requests planned, one at a time for large requests: the 3 after the failed one never get sent
        assertEquals(1, failing.requests().size());
      }
    }
  }

  static class CachedTest extends RangeFileTest {

    Path path;

    @Override
    RangeFile open() throws IOException {
      path = tmp.resolve("file.parquet");
      return RangeFile.open(path, URL, TAIL_BYTES, DownloadMode.CACHED, false, downloader);
    }

    private RangeFile reopen() throws IOException {
      file.close();
      downloader.clearRequests();
      return file = RangeFile.open(path, URL, TAIL_BYTES, DownloadMode.CACHED, false, downloader);
    }

    @Test
    void testReusesCachedRanges() throws IOException {
      file.read(10, 100);
      reopen();
      // only checks that the remote file didn't change
      assertEquals(List.of(new StubDownloader.Request(URL, "bytes=0-0", null, null)), downloader.requests());
      assertReads(file, 10, 100);
      assertReads(file, CONTENTS.length - 100L, 100);
      assertEquals(1, downloader.requests().size());

      assertReads(file, 0, 200);
      assertEquals(2, downloader.requests().size());
    }

    @Test
    void testStartsOverWhenRemoteFileChanged() throws IOException {
      file.read(10, 100);
      byte[] changed = contents(CONTENTS.length);
      changed[20] = 1;
      downloader.put(URL, changed);
      reopen();
      assertArrayEquals(Arrays.copyOfRange(changed, 10, 110), file.read(10, 100));
      assertEquals(2, downloader.requests().size());
    }

    @Test
    void testBecomesCompleteFile() throws IOException {
      file.read(0, CONTENTS.length - TAIL_BYTES);
      assertFalse(Files.exists(CachedRangeFile.rangesPath(path)));
      assertTrue(Files.exists(CachedRangeFile.metaPath(path)));
      assertArrayEquals(CONTENTS, Files.readAllBytes(path));

      reopen();
      assertInstanceOf(LocalRangeFile.class, file);
      assertEquals(List.of(), downloader.requests());
    }

    @Test
    void testRefreshReplacesChangedCompleteFile() throws IOException {
      file.read(0, CONTENTS.length);
      file.close();

      file = RangeFile.open(path, URL, TAIL_BYTES, DownloadMode.CACHED, true, downloader);
      assertInstanceOf(LocalRangeFile.class, file);
      file.close();

      downloader.put(URL, CONTENTS, "\"new\"", null);
      file = RangeFile.open(path, URL, TAIL_BYTES, DownloadMode.STREAMING, true, downloader);
      assertInstanceOf(HttpRangeFile.class, file);
      assertTrue(Files.exists(path), "streaming leaves the local copy alone");

      file = RangeFile.open(path, URL, TAIL_BYTES, DownloadMode.CACHED, true, downloader);
      assertInstanceOf(CachedRangeFile.class, file);
      assertTrue(CachedRangeFile.isPartial(path));
    }

    @Test
    void testReadsOfflineFromPartialCache() throws IOException {
      file.read(10, 100);
      file.close();
      file = RangeFile.openLocal(path);
      assertReads(file, 10, 100);
      var exception = assertThrows(IOException.class, () -> file.read(0, 100));
      assertTrue(exception.getMessage().contains("--download"), exception.getMessage());
    }

    @Test
    void testIgnoresTruncatedRangesLine() throws IOException {
      file.read(10, 100);
      file.close();
      // a crash while appending a line can leave just the start of it
      Files.writeString(CachedRangeFile.rangesPath(path), "200", StandardCharsets.UTF_8, StandardOpenOption.APPEND);
      reopen();
      assertReads(file, 10, 100);
      assertEquals(1, downloader.requests().size());
    }

    @Test
    void testCountsOnlyBytesFetchedFromRemote() throws IOException {
      file.read(10, 100);
      reopen();
      file.read(10, 100);
      assertEquals(1, file.bytesFetched(), "only the validation request");
      assertEquals(1, file.requestsMade());
      file.read(200, 100);
      assertEquals(101, file.bytesFetched());
      assertEquals(2, file.requestsMade());
    }

    @Test
    void testToFetchExcludesCachedRanges() throws IOException {
      file.read(10, 100);
      assertFalse(file.needsFetch(20, 50));
      assertEquals(RequestPlanner.Plan.EMPTY, file.planFetch(List.of(range(20, 50))));
      assertTrue(file.needsFetch(0, 210), "a partly cached read fetches all of it");
      assertEquals(List.of(range(0, 210)), file.planFetch(List.of(range(0, 210))).httpRequests());
      assertFalse(file.needsFetch(CONTENTS.length - 10L, 10), "the tail is cached when the file is created");
      file.close();
      file = RangeFile.openLocal(path);
      assertFalse(file.needsFetch(0, 210), "nothing gets fetched offline");
    }

    @ParameterizedTest
    @EnumSource(names = {"CACHED", "STREAMING"})
    void testDownloadsWholeFileWithoutRangeSupport(DownloadMode mode) throws IOException {
      file.close();
      Files.delete(path);
      Files.delete(CachedRangeFile.rangesPath(path));
      downloader.ignoreRanges();
      FileUtils.delete(path, CachedRangeFile.metaPath(path));
      file = RangeFile.open(path, URL, TAIL_BYTES, mode, false, downloader);
      assertInstanceOf(LocalRangeFile.class, file);
      assertReads(file, 0, CONTENTS.length);
      file.close();
    }

    @Test
    void testCachesGapsFetchedBetweenRanges() throws IOException {
      // 200 KB needed: the 500 byte gap between the ranges gets fetched with them, and cached too
      var reads = file.readLater(List.of(range(0, 100_000), range(100_500, 100_000)));
      reads.get(0).get();
      reads.get(1).get();
      reopen();
      assertReads(file, 0, 200_500);
      assertEquals(1, downloader.requests().size(), "only the validation request");
    }

    @Test
    void testReadsCachedAndMissingRangesTogether() throws IOException {
      file.read(10, 100);
      downloader.clearRequests();
      var reads = file.readLater(List.of(range(10, 100), range(500_000, 100)));
      assertArrayEquals(slice(10, 100), reads.get(0).get());
      assertArrayEquals(slice(500_000, 100), reads.get(1).get());
      assertEquals(List.of("bytes=500000-500099"), ranges());
    }
  }
}
