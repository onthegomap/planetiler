package com.onthegomap.planetiler.util;

import static com.google.common.net.HttpHeaders.*;

import com.onthegomap.planetiler.config.PlanetilerConfig;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import javax.net.ssl.SSLSession;

/**
 * A {@link Downloader} that answers range requests from files held in memory instead of a real server, so tests can
 * read "remote" files without network access and check the requests that were made.
 */
public class StubRangeDownloader extends Downloader {

  private static final Pattern RANGE_PATTERN = Pattern.compile("bytes=(\\d*)-(\\d*)");
  private final Map<String, File> files = new ConcurrentHashMap<>();
  private final Map<String, String> redirects = new ConcurrentHashMap<>();
  private final List<Request> requests = new ArrayList<>();
  private final AtomicInteger failuresLeft = new AtomicInteger(0);
  private volatile int failureStatus = 503;
  private volatile boolean ignoreRanges = false;
  private volatile boolean ignoreSuffixRanges = false;

  /** A request the stub received: its URL and the headers relevant to range requests. */
  public record Request(String url, String range, String ifMatch, String ifUnmodifiedSince) {}

  private record File(byte[] bytes, String etag, String lastModified) {}

  public StubRangeDownloader(PlanetilerConfig config) {
    super(config, config.downloadChunkSizeMB() * 1_000_000L);
  }

  /** Serves {@code bytes} at {@code url} with ETag and Last-Modified derived from its contents. */
  public StubRangeDownloader put(String url, byte[] bytes) {
    int hash = Arrays.hashCode(bytes);
    return put(url, bytes, "\"" + Integer.toHexString(hash) + "\"", "Thu, 01 Jan 2026 00:00:" + Math.abs(hash % 60) +
      " GMT");
  }

  /** Serves {@code bytes} at {@code url} with the given validators, either of which may be {@code null}. */
  public StubRangeDownloader put(String url, byte[] bytes, String etag, String lastModified) {
    files.put(url, new File(bytes, etag, lastModified));
    return this;
  }

  /** Answers requests to {@code from} with a redirect to {@code to}. */
  public StubRangeDownloader redirect(String from, String to) {
    redirects.put(from, to);
    return this;
  }

  /** Answers the next {@code count} requests with HTTP {@code status}. */
  public StubRangeDownloader failNext(int count, int status) {
    failureStatus = status;
    failuresLeft.set(count);
    return this;
  }

  /** Answers every request with the whole file, like a server without range support. */
  public StubRangeDownloader ignoreRanges() {
    ignoreRanges = true;
    return this;
  }

  /** Answers suffix range requests ({@code bytes=-n}) with the whole file, like a server that doesn't support them. */
  public StubRangeDownloader ignoreSuffixRanges() {
    ignoreSuffixRanges = true;
    return this;
  }

  /** Returns the requests received so far, in order. */
  public synchronized List<Request> requests() {
    return List.copyOf(requests);
  }

  /** Forgets the requests received so far. */
  public synchronized void clearRequests() {
    requests.clear();
  }

  @Override
  protected void retrySleep() {
    // retry right away
  }

  @Override
  HttpResponse<InputStream> send(HttpRequest request) {
    String url = request.uri().toString();
    var headers = request.headers();
    String range = headers.firstValue(RANGE).orElse(null);
    String ifMatch = headers.firstValue(IF_MATCH).orElse(null);
    String ifUnmodifiedSince = headers.firstValue(IF_UNMODIFIED_SINCE).orElse(null);
    synchronized (this) {
      requests.add(new Request(url, range, ifMatch, ifUnmodifiedSince));
    }
    if (failuresLeft.getAndDecrement() > 0) {
      return response(request, failureStatus, Map.of(), new byte[0]);
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
      return response(request, 200, validators, file.bytes);
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
    return response(request, 206, responseHeaders, Arrays.copyOfRange(file.bytes, start, end + 1));
  }

  private static HttpResponse<InputStream> response(HttpRequest request, int status, Map<String, String> headers,
    byte[] body) {
    Map<String, List<String>> headerLists = new HashMap<>();
    headers.forEach((key, value) -> headerLists.put(key, List.of(value)));
    var httpHeaders = HttpHeaders.of(headerLists, (a, b) -> true);
    return new HttpResponse<>() {
      @Override
      public int statusCode() {
        return status;
      }

      @Override
      public HttpRequest request() {
        return request;
      }

      @Override
      public Optional<HttpResponse<InputStream>> previousResponse() {
        return Optional.empty();
      }

      @Override
      public HttpHeaders headers() {
        return httpHeaders;
      }

      @Override
      public InputStream body() {
        return new ByteArrayInputStream(body);
      }

      @Override
      public Optional<SSLSession> sslSession() {
        return Optional.empty();
      }

      @Override
      public URI uri() {
        return request.uri();
      }

      @Override
      public HttpClient.Version version() {
        return HttpClient.Version.HTTP_1_1;
      }
    };
  }
}
