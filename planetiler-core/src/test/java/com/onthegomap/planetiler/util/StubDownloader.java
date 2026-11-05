package com.onthegomap.planetiler.util;

import static com.google.common.net.HttpHeaders.*;

import com.onthegomap.planetiler.config.PlanetilerConfig;
import java.io.ByteArrayInputStream;
import java.net.http.HttpHeaders;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

/**
 * A {@link Downloader} that answers requests from files held in memory instead of a real server, so tests can read and
 * download "remote" files without network access and check the requests that were made. Any request for a URL it
 * doesn't serve gets a 404, so nothing can reach the real network.
 */
public class StubDownloader extends Downloader {
  public final List<Downloader.Request> sent = new CopyOnWriteArrayList<>();

  private static final Pattern RANGE_PATTERN = Pattern.compile("bytes=(\\d*)-(\\d*)");
  private final Map<String, File> files = new ConcurrentHashMap<>();
  private final Map<String, String> redirects = new ConcurrentHashMap<>();
  private final List<Request> requests = new ArrayList<>();
  private final AtomicInteger failuresLeft = new AtomicInteger(0);
  private final AtomicInteger truncationsLeft = new AtomicInteger(0);
  private volatile int truncateAfterBytes = 0;
  private volatile int failureStatus = 503;
  private volatile Map<String, String> failureHeaders = Map.of();
  private final List<Duration> retryWaits = new ArrayList<>();
  private volatile boolean ignoreRanges = false;
  private volatile boolean ignoreSuffixRanges = false;
  private volatile boolean rejectSuffixRanges = false;
  private volatile boolean omitContentLength = false;

  /** A request the stub received: its URL and the headers relevant to range requests. */
  public record Request(String url, String range, String ifMatch, String ifUnmodifiedSince) {}

  private record File(byte[] bytes, String etag, String lastModified) {}

  public StubDownloader(PlanetilerConfig config) {
    this(config, config.downloadChunkSizeMB() * 1_000_000L);
  }

  public StubDownloader(PlanetilerConfig config, long maxRequestBytes) {
    super(config, maxRequestBytes);
  }

  public StubDownloader put(String url, byte[] bytes) {
    int hash = Arrays.hashCode(bytes);
    return put(url, bytes, "\"" + Integer.toHexString(hash) + "\"", "Thu, 01 Jan 2026 00:00:" + Math.abs(hash % 60) +
      " GMT");
  }

  public StubDownloader put(String url, byte[] bytes, String etag, String lastModified) {
    files.put(url, new File(bytes, etag, lastModified));
    return this;
  }

  public StubDownloader redirect(String from, String to) {
    redirects.put(from, to);
    return this;
  }

  public StubDownloader failNext(int count, int status) {
    return failNext(count, status, Map.of());
  }

  public StubDownloader failNext(int count, int status, Map<String, String> headers) {
    failureStatus = status;
    failureHeaders = headers;
    failuresLeft.set(count);
    return this;
  }

  public synchronized List<Duration> retryWaits() {
    return List.copyOf(retryWaits);
  }

  // like a dropped connection.
  public StubDownloader truncateNext(int count, int bytes) {
    truncateAfterBytes = bytes;
    truncationsLeft.set(count);
    return this;
  }

  public StubDownloader ignoreRanges() {
    ignoreRanges = true;
    return this;
  }

  public StubDownloader ignoreSuffixRanges() {
    ignoreSuffixRanges = true;
    return this;
  }

  // like GitHub release downloads.
  public StubDownloader rejectSuffixRanges() {
    rejectSuffixRanges = true;
    return this;
  }

  public StubDownloader omitContentLength() {
    omitContentLength = true;
    return this;
  }

  public synchronized List<Request> requests() {
    return List.copyOf(requests);
  }

  public synchronized void clearRequests() {
    requests.clear();
  }

  @Override
  protected synchronized void retrySleep(Duration wait) {
    retryWaits.add(wait);
  }

  @Override
  Response sendOverNetwork(Downloader.Request request) {
    sent.add(request);
    String url = request.url();
    String range = request.headers().get(RANGE);
    String ifMatch = request.headers().get(IF_MATCH);
    String ifUnmodifiedSince = request.headers().get(IF_UNMODIFIED_SINCE);
    synchronized (this) {
      requests.add(new Request(url, range, ifMatch, ifUnmodifiedSince));
    }
    if (failuresLeft.getAndDecrement() > 0) {
      return response(request, failureStatus, failureHeaders, new byte[0]);
    }
    if (redirects.containsKey(url)) {
      return response(request, 302, Map.of(LOCATION, redirects.get(url)), new byte[0]);
    }
    File file = files.get(url);
    if (file == null) {
      return response(request, 404, Map.of(), new byte[0]);
    }
    Map<String, String> validators = new HashMap<>();
    if (file.etag != null) {
      validators.put(ETAG, file.etag);
    }
    if (file.lastModified != null) {
      validators.put(LAST_MODIFIED, file.lastModified);
    }
    if ((ifMatch != null && !ifMatch.equals(file.etag)) ||
      (ifUnmodifiedSince != null && !ifUnmodifiedSince.equals(file.lastModified))) {
      return response(request, 412, Map.of(), new byte[0]);
    }
    if (range == null || ignoreRanges || (ignoreSuffixRanges && range.startsWith("bytes=-"))) {
      Map<String, String> responseHeaders = new HashMap<>(validators);
      if (!omitContentLength) {
        responseHeaders.put(CONTENT_LENGTH, Integer.toString(file.bytes.length));
      }
      return response(request, 200, responseHeaders, maybeTruncate(file.bytes));
    }
    if (rejectSuffixRanges && range.startsWith("bytes=-")) {
      return response(request, 501, Map.of(), new byte[0]);
    }
    var matcher = RANGE_PATTERN.matcher(range);
    if (!matcher.matches()) {
      return response(request, 416, Map.of(), new byte[0]);
    }
    int length = file.bytes.length;
    int start;
    int end;
    if (matcher.group(1).isEmpty()) {
      start = Math.max(0, length - Integer.parseInt(matcher.group(2)));
      end = length - 1;
    } else {
      start = Integer.parseInt(matcher.group(1));
      end = matcher.group(2).isEmpty() ? length - 1 : Math.min(length - 1, Integer.parseInt(matcher.group(2)));
    }
    Map<String, String> responseHeaders = new HashMap<>(validators);
    responseHeaders.put(CONTENT_RANGE, "bytes " + start + "-" + end + "/" + length);
    return response(request, 206, responseHeaders, maybeTruncate(Arrays.copyOfRange(file.bytes, start, end + 1)));
  }

  private byte[] maybeTruncate(byte[] body) {
    return truncationsLeft.getAndDecrement() > 0 ? Arrays.copyOf(body, Math.min(body.length, truncateAfterBytes)) :
      body;
  }

  /** Returns a response to {@code request} with {@code status}, {@code headers} and {@code body}. */
  static Response response(Downloader.Request request, int status, Map<String, String> headers, byte[] body) {
    Map<String, List<String>> headerLists = new HashMap<>();
    headers.forEach((key, value) -> headerLists.put(key, List.of(value)));
    return new Response(request.url(), status, HttpHeaders.of(headerLists, (a, b) -> true),
      new ByteArrayInputStream(body));
  }
}
