package com.onthegomap.planetiler.util;

import static com.google.common.net.HttpHeaders.CONTENT_RANGE;
import static com.google.common.net.HttpHeaders.RETRY_AFTER;
import static org.junit.jupiter.api.Assertions.*;

import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.reader.remote.CachedRangeFile;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class DownloaderTest {

  @TempDir
  Path path;
  private final PlanetilerConfig config = PlanetilerConfig.defaults();

  private static final byte[] RANGE_BYTES = "0123456789".getBytes(StandardCharsets.UTF_8);

  private static String string(Downloader.RangeResponse response) {
    return new String(response.bytes(), StandardCharsets.UTF_8);
  }

  private static byte[] bytes(String string) {
    return string.getBytes(StandardCharsets.UTF_8);
  }

  private static void download(Downloader downloader, String url, Path dest) throws Exception {
    downloader.downloadIfNecessary(new Downloader.ResourceToDownload("resource", url, dest)).get();
  }

  private static List<String> ranges(StubDownloader downloader) {
    return downloader.requests().stream().map(StubDownloader.Request::range).toList();
  }

  @ParameterizedTest
  @CsvSource({
    "true,0",
    "true,1",
    "true,4",
    "false,0",
    "false,2",
  })
  void testDownload(boolean supportsRanges, int redirects) throws Exception {
    Path dest = path.resolve("out");
    String url = "http://url";
    String initialUrl = url + (redirects > 0 ? "#" + redirects : "");
    var downloader = new StubDownloader(config);
    for (int i = redirects; i > 0; i--) {
      downloader.redirect(url + "#" + i, i == 1 ? url : url + "#" + (i - 1));
    }
    if (!supportsRanges) {
      downloader.ignoreRanges();
    }

    // fails if no data
    var resource1 = new Downloader.ResourceToDownload("resource", initialUrl, dest);
    assertThrows(ExecutionException.class, () -> downloader.downloadIfNecessary(resource1).get());
    assertFalse(Files.exists(dest));

    // succeeds with data
    downloader.put(url, bytes("0123456789"));
    downloader.downloadIfNecessary(new Downloader.ResourceToDownload("resource", initialUrl, dest)).get();
    assertEquals("0123456789", Files.readString(dest));
    assertFalse(CachedRangeFile.isPartial(dest));

    // does not download again if the remote file is the same
    downloader.clearRequests();
    downloader.downloadIfNecessary(new Downloader.ResourceToDownload("resource", initialUrl, dest)).get();
    assertEquals("0123456789", Files.readString(dest));
    if (supportsRanges) {
      // only the request that checks the remote file's version, through each redirect
      assertEquals(Collections.nCopies(redirects + 1, "bytes=0-0"), ranges(downloader));
    } else {
      // the whole file comes back for the requests that check its version, but none downloads it
      assertFalse(ranges(downloader).contains(null), ranges(downloader).toString());
    }

    // downloads again if the remote file changes
    downloader.put(url, bytes("54321"));
    downloader.downloadIfNecessary(new Downloader.ResourceToDownload("resource", initialUrl, dest)).get();
    assertEquals("54321", Files.readString(dest));
  }

  @ParameterizedTest
  @CsvSource({
    "5, true",
    "6, false"
  })
  void testRetry5xOK(int failures, boolean ok) throws Exception {
    String url = "http://url";
    Path dest = path.resolve("out");
    var downloader = new StubDownloader(config).put(url, bytes("abc")).failNext(failures, 503);
    var future = downloader.downloadIfNecessary(new Downloader.ResourceToDownload("resource", url, dest));
    if (ok) {
      future.get();
      assertEquals("abc", Files.readString(dest));
    } else {
      var exception = assertThrows(ExecutionException.class, future::get);
      assertInstanceOf(IOException.class, ExceptionUtils.getRootCause(exception));
      assertFalse(Files.exists(dest));
    }
  }

  @Test
  void testDownloadFailsIfTooBig() {
    // a server that reports a huge length for the file
    var downloader = new StubDownloader(config) {
      @Override
      Response sendOverNetwork(Downloader.Request request) {
        return response(request, 206, Map.of(CONTENT_RANGE, "bytes 0-0/" + (Long.MAX_VALUE / 2)), new byte[1]);
      }
    };
    Path dest = path.resolve("out");

    var resource = new Downloader.ResourceToDownload("resource", "http://url", dest);
    var exception = assertThrows(ExecutionException.class, () -> downloader.downloadIfNecessary(resource).get());
    assertInstanceOf(IllegalStateException.class, exception.getCause());
    assertTrue(exception.getMessage().contains("--force"), exception.getMessage());
  }

  @Test
  void testFetchTailAndRange() throws Exception {
    var downloader = new StubDownloader(config).put("http://url", RANGE_BYTES, "\"v1\"", "date1");

    var tail = downloader.fetchTail("http://url", 3);
    assertEquals("789", string(tail));
    assertEquals(7, tail.offset());
    assertEquals(new Downloader.RemoteVersion(10, "\"v1\"", "date1"), tail.version());

    byte[] result = new byte[4];
    fetch(downloader, tail.version(), 2, result);
    assertEquals("2345", new String(result, StandardCharsets.UTF_8));
  }

  private static void fetch(Downloader downloader, Downloader.RemoteVersion version, long offset, byte[] result)
    throws Exception {
    byte[] bytes = downloader.fetchBytes("http://url", version, new Downloader.ByteRange(offset, result.length), _ -> {
    }).get();
    System.arraycopy(bytes, 0, result, 0, bytes.length);
  }

  @Test
  void testFetchRangeFollowsRedirects() throws Exception {
    var downloader = new StubDownloader(config)
      .redirect("http://a", "/b")
      .put("http://a/b", RANGE_BYTES);
    var response = downloader.fetchTail("http://a", 2);
    assertEquals("http://a/b", response.url());
    assertEquals("89", string(response));
  }

  @Test
  void testFetchRangeSendsPreconditions() throws Exception {
    var downloader = new StubDownloader(config).put("http://url", RANGE_BYTES, "W/\"weak\"", "date1");
    var version = downloader.fetchTail("http://url", 1).version();
    fetch(downloader, version, 0, new byte[1]);
    assertThrows(ExecutionException.class,
      () -> fetch(downloader, new Downloader.RemoteVersion(10, "\"strong\"", "date1"), 0, new byte[1]));

    var requests = downloader.requests();
    // If-Match only works with strong ETags, so a weak one falls back to Last-Modified
    assertEquals(new StubDownloader.Request("http://url", "bytes=0-0", null, "date1"), requests.get(1));
    assertEquals(new StubDownloader.Request("http://url", "bytes=0-0", "\"strong\"", null), requests.get(2));
  }

  @Test
  void testFetchRangeFailsWhenFileChanged() throws Exception {
    var downloader = new StubDownloader(config).put("http://url", RANGE_BYTES, "\"v1\"", null);
    var version = downloader.fetchTail("http://url", 1).version();
    downloader.put("http://url", RANGE_BYTES, "\"v2\"", null);

    var exception = assertThrows(ExecutionException.class, () -> fetch(downloader, version, 0, new byte[1]));
    assertInstanceOf(Downloader.RemoteFileChangedException.class, exception.getCause());
    assertTrue(exception.getCause().getMessage().contains("--download-fail-if-changed=false"),
      exception.getCause().getMessage());
  }

  @Test
  void testFetchRangeRetriesServerErrors() throws Exception {
    var downloader = new StubDownloader(config).put("http://url", RANGE_BYTES).failNext(2, 503);
    assertEquals("9", string(downloader.fetchTail("http://url", 1)));
    assertEquals(3, downloader.requests().size());

    downloader.failNext(config.httpRetries() + 1, 429);
    assertThrows(IOException.class, () -> downloader.fetchTail("http://url", 1));
  }

  @Test
  void testFetchRangeFailsWithoutRangeSupport() {
    var downloader = new StubDownloader(config).put("http://url", RANGE_BYTES).ignoreRanges();
    var exception =
      assertThrows(Downloader.RangesNotSupportedException.class, () -> downloader.fetchTail("http://url", 1));
    assertTrue(exception.getMessage().contains("does not support range requests"), exception.getMessage());
  }

  @Test
  void testRemoteVersionMatchesOnlyValidatorsBothHave() {
    var version = new Downloader.RemoteVersion(10, "\"a\"", "date1");
    assertTrue(version.matches(new Downloader.RemoteVersion(10, "\"a\"", "date1")));
    assertTrue(version.matches(new Downloader.RemoteVersion(10, null, null)));
    assertFalse(version.matches(new Downloader.RemoteVersion(11, "\"a\"", "date1")));
    assertFalse(version.matches(new Downloader.RemoteVersion(10, "\"b\"", "date1")));
    assertFalse(version.matches(new Downloader.RemoteVersion(10, null, "date2")));
  }

  @ParameterizedTest
  @CsvSource({"true", "false"})
  void testFetchTailWithoutSuffixRangeSupport(boolean rejects) throws Exception {
    var downloader = new StubDownloader(config).put("http://url", RANGE_BYTES);
    if (rejects) {
      downloader.rejectSuffixRanges();
    } else {
      downloader.ignoreSuffixRanges();
    }

    var tail = downloader.fetchTail("http://url", 3);
    assertEquals("789", string(tail));
    assertEquals(7, tail.offset());
    assertEquals(10, tail.version().length());
    assertEquals(List.of("bytes=-3", "bytes=0-0", "bytes=7-9"), ranges(downloader));
  }

  @Test
  void testFetchResumesAfterDroppedConnection() throws Exception {
    var downloader = new StubDownloader(config).put("http://url", RANGE_BYTES);
    var version = downloader.fetchTail("http://url", 1).version();
    downloader.clearRequests();
    downloader.truncateNext(1, 3);

    byte[] result = new byte[8];
    fetch(downloader, version, 1, result);
    assertEquals("12345678", new String(result, StandardCharsets.UTF_8));
    assertEquals(List.of("bytes=1-8", "bytes=4-8"), ranges(downloader));
  }

  @Test
  void testSplitsLargeFetches() {
    var threeThreads = PlanetilerConfig.from(Arguments.of("download_threads", "3"));
    // requests can't be larger than the largest request size
    assertEquals(
      List.of(new Downloader.ByteRange(0, 4), new Downloader.ByteRange(4, 4), new Downloader.ByteRange(8, 2)),
      new StubDownloader(threeThreads, 4).planRequests(List.of(new Downloader.ByteRange(0, 10))));

    // otherwise reads get split into download_threads requests, but none smaller than the minimum
    var downloader = new StubDownloader(threeThreads, Long.MAX_VALUE);
    long min = Downloader.MIN_REQUEST_BYTES;
    assertEquals(List.of(new Downloader.ByteRange(5, 10)),
      downloader.planRequests(List.of(new Downloader.ByteRange(5, 10))));
    assertEquals(2, downloader.planRequests(List.of(new Downloader.ByteRange(0, 2 * min))).size());
    assertEquals(3, downloader.planRequests(List.of(new Downloader.ByteRange(0, 10 * min))).size());

    // several ranges share download_threads
    assertEquals(3, downloader.planRequests(List.of(new Downloader.ByteRange(0, 10 * min),
      new Downloader.ByteRange(20 * min, 10 * min), new Downloader.ByteRange(40 * min, 10 * min))).size());
  }

  @Test
  void testDownloadResumesPartialFile() throws Exception {
    byte[] contents = new byte[3_000_000];
    for (int i = 0; i < contents.length; i++) {
      contents[i] = (byte) i;
    }
    var downloader = new StubDownloader(config).put("http://url", contents);
    Path dest = path.resolve("out");
    // a cached read (or an interrupted download) fetched part of the file, and its tail when it got opened
    try (var partial = CachedRangeFile.open(dest, "http://url", 1 << 20, downloader)) {
      partial.read(0, 1000);
    }
    assertTrue(CachedRangeFile.isPartial(dest));
    downloader.clearRequests();

    var resource = new Downloader.ResourceToDownload("out", "http://url", dest);
    downloader.downloadIfNecessary(resource).get();

    assertFalse(CachedRangeFile.isPartial(dest));
    assertArrayEquals(contents, Files.readAllBytes(dest));
    long tailStart = contents.length - (1 << 20);
    assertEquals(List.of("bytes=0-0", "bytes=1000-" + (tailStart - 1)), ranges(downloader));
    // progress counts the whole file, starting from the bytes fetched before
    assertEquals(contents.length, resource.length().get());
    assertEquals(contents.length, resource.bytesDownloaded());
  }

  @Test
  void testDownloadStartsOverWhenRemoteFileChanged() throws Exception {
    var downloader = new StubDownloader(config).put("http://url", RANGE_BYTES);
    Path dest = path.resolve("out");
    download(downloader, "http://url", dest);
    downloader.put("http://url", bytes("abcdefghij"));

    download(downloader, "http://url", dest);

    assertEquals("abcdefghij", Files.readString(dest));
  }

  @Test
  void testDownloadWithoutRangeSupport() throws Exception {
    var downloader = new StubDownloader(config).put("http://url", RANGE_BYTES).ignoreRanges();
    Path dest = path.resolve("out");

    download(downloader, "http://url", dest);

    assertEquals("0123456789", Files.readString(dest));
    assertFalse(CachedRangeFile.isPartial(dest));
    assertNull(downloader.requests().getLast().range());
  }

  @Test
  void testCompleteFileWithoutVersionComparesLength() throws Exception {
    var downloader = new StubDownloader(config).put("http://url", RANGE_BYTES);
    Path dest = path.resolve("out");
    // left by an older version of planetiler that didn't save the remote file's version
    Files.writeString(dest, "same size!");

    download(downloader, "http://url", dest);

    assertEquals("same size!", Files.readString(dest));
  }

  @Test
  void testOpenStream() throws Exception {
    var downloader = new StubDownloader(config).redirect("http://a", "http://b").put("http://b", RANGE_BYTES);
    try (var in = downloader.openStream("http://a")) {
      assertArrayEquals(RANGE_BYTES, in.readAllBytes());
    }
    assertThrows(IOException.class, () -> downloader.openStream("http://missing"));
    assertEquals(1, downloader.requests().stream().filter(r -> r.url().equals("http://missing")).count(),
      "a 404 doesn't get retried");
  }

  @Test
  void testPartialDownloadCompletedByServerWithoutRangeSupport() throws Exception {
    var downloader = new StubDownloader(config).put("http://url", RANGE_BYTES);
    Path dest = path.resolve("out");
    try (var partial = CachedRangeFile.open(dest, "http://url", 0, downloader)) {
      partial.read(0, 2);
    }
    downloader.ignoreRanges();

    download(downloader, "http://url", dest);

    assertEquals("0123456789", Files.readString(dest));
    assertFalse(CachedRangeFile.isPartial(dest));
  }

  @Test
  void testRetryWaitsAsLongAsTheServerAsks() throws Exception {
    var downloader = new StubDownloader(config).put("http://url", RANGE_BYTES)
      .failNext(1, 429, Map.of(RETRY_AFTER, "120"));
    assertEquals("9", string(downloader.fetchTail("http://url", 1)));
    assertEquals(List.of(Duration.ofMinutes(2)), downloader.retryWaits());

    // a shorter wait than http_retry_wait doesn't make it retry sooner
    downloader.failNext(1, 503, Map.of(RETRY_AFTER, "0"));
    downloader.fetchTail("http://url", 1);
    assertEquals(config.httpRetryWait(), downloader.retryWaits().getLast());
  }

  @Test
  void testCapsWaitAtMaxWait() throws Exception {
    var downloader = new StubDownloader(config).put("http://url", RANGE_BYTES)
      .failNext(1, 429, Map.of(RETRY_AFTER, "3600"));
    assertEquals("9", string(downloader.fetchTail("http://url", 1)));
    assertEquals(List.of(config.httpRetryMaxWait()), downloader.retryWaits());
  }

  @Test
  void testParseRetryAfter() {
    Instant now = Instant.ofEpochSecond(1);
    assertEquals(Duration.ofSeconds(120), Downloader.parseRetryAfter("120", now));
    assertEquals(Duration.ofSeconds(30), Downloader.parseRetryAfter("Thu, 01 Jan 1970 00:00:31 GMT", now));
    assertEquals(Duration.ZERO, Downloader.parseRetryAfter("Thu, 01 Jan 1970 00:00:00 GMT", now));
    assertNull(Downloader.parseRetryAfter("soon", now));
    assertNull(Downloader.parseRetryAfter("-5", now));
    assertNull(Downloader.parseRetryAfter("", now));
  }

  @Test
  void testSendWithRetriesReadsBodyAndDoesNotRetryClientErrors() throws Exception {
    var downloader = new StubDownloader(config) {
      @Override
      Response sendOverNetwork(Downloader.Request request) {
        super.sendOverNetwork(request);
        return request.body().equals("bad") ?
          response(request, 400, Map.of(), bytes("Query is malformed:\n line 1")) :
          response(request, 200, Map.of(), bytes("result of " + request.body()));
      }
    };
    var request = new Downloader.Request("http://url", Map.of(), "query");
    downloader.failNext(1, 503);
    assertEquals("result of query",
      downloader.sendWithRetries(request, body -> new String(body.readAllBytes(), StandardCharsets.UTF_8)));

    downloader.clearRequests();
    var badRequest = new Downloader.Request("http://url", Map.of(), "bad");
    var exception = assertThrows(IOException.class, () -> downloader.sendWithRetries(badRequest, _ -> null));
    assertTrue(exception.getMessage().contains("400 Query is malformed: line 1"), exception.getMessage());
    assertEquals(1, downloader.requests().size(), "a 400 doesn't get retried");
  }

  @Test
  void testHeaderNamesDifferingInCaseGetMerged() {
    Map<String, List<String>> fields = new HashMap<>();
    fields.put(null, List.of("HTTP/1.1 200 OK"));
    fields.put("Vary", List.of("Accept"));
    fields.put("vary", List.of("Origin"));
    var headers = Downloader.toHttpHeaders(fields);
    assertEquals(1, headers.map().size());
    assertEquals(Set.of("Accept", "Origin"), Set.copyOf(headers.allValues("vary")));
  }

  @Test
  void testOpenStreamReadsFileUrls() throws Exception {
    Path file = path.resolve("file.txt");
    Files.writeString(file, "local");
    var downloader = new StubDownloader(config);
    try (var in = downloader.openStream(file.toUri().toString())) {
      assertEquals("local", new String(in.readAllBytes(), StandardCharsets.UTF_8));
    }
    assertEquals(List.of(), downloader.requests());
  }

  @Test
  void testDownloadWithoutRangeSupportOrLength() throws Exception {
    var downloader = new StubDownloader(config).put("http://url", RANGE_BYTES).ignoreRanges().omitContentLength();
    Path dest = path.resolve("out");

    download(downloader, "http://url", dest);

    assertEquals("0123456789", Files.readString(dest));
    assertFalse(CachedRangeFile.isPartial(dest));
    assertTrue(Files.readString(CachedRangeFile.metaPath(dest)).contains("\"length\":10"));
  }

  @Test
  void testInterruptedDownloadWithoutRangeSupportStartsOver() throws Exception {
    var noRetries = PlanetilerConfig.from(Arguments.of(Map.of("http_retries", "0")));
    var downloader = new StubDownloader(noRetries).put("http://url", RANGE_BYTES).ignoreRanges();
    Path dest = path.resolve("out");
    downloader.truncateNext(3, 4);

    assertThrows(ExecutionException.class, () -> download(downloader, "http://url", dest));
    assertTrue(CachedRangeFile.isPartial(dest));

    download(downloader, "http://url", dest);
    assertEquals("0123456789", Files.readString(dest));
    assertFalse(CachedRangeFile.isPartial(dest));
  }

  @Test
  void testDownloadEmptyFileWithoutRangeSupport() throws Exception {
    var downloader = new StubDownloader(config).put("http://url", new byte[0]).ignoreRanges();
    Path dest = path.resolve("out");

    download(downloader, "http://url", dest);

    assertEquals(0, Files.size(dest));
    assertFalse(CachedRangeFile.isPartial(dest));
  }

  @Test
  void testFetchSkipsRemainingRequestsAfterOneFails() {
    var oneThreadNoRetries =
      PlanetilerConfig.from(Arguments.of(Map.of("download_threads", "1", "http_retries", "0")));
    var downloader = new StubDownloader(oneThreadNoRetries, 4).put("http://url", RANGE_BYTES).failNext(1, 503);
    var version = new Downloader.RemoteVersion(RANGE_BYTES.length, null, null);
    var result = new byte[10];

    assertThrows(ExecutionException.class, () -> fetch(downloader, version, 0, result));
    // 3 requests planned, the 2 after the failed one never get sent
    assertEquals(1, downloader.requests().size());
  }
}
